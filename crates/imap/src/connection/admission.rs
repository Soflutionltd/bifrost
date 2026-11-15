//! One admission policy, read against the handle snapshot and driver live state.

use super::ImapConnection;
use super::driver::ConnectionStateSnapshot;
use crate::codec::encode::EncodeOptions;
use crate::error::{Error, LocalFailure, StateRequirement};
use crate::types::{Capability, Command};

/// The refusal for a command whose prerequisite capability is not usable on
/// this connection (I6). One wording for every command: `<cmd> requires <cap>`.
fn missing(cmd: &str, cap: &str) -> crate::Error {
    crate::Error::MissingCapability(crate::error::CapabilityFailure::new(
        cap.into(),
        crate::error::CapabilityFailureReason::NotAdvertised,
        format!("{cmd} requires {cap}"),
    ))
}

pub(super) struct SessionView<'a> {
    pub(super) snapshot: &'a ConnectionStateSnapshot,
    pub(super) tls_active: bool,
}

impl SessionView<'_> {
    fn supports(&self, cap: &Capability) -> bool {
        crate::types::profile::supports(&self.snapshot.capabilities, &self.snapshot.enabled, cap)
    }

    fn require(&self, cap: Capability) -> Result<(), Error> {
        if self.supports(&cap) {
            return Ok(());
        }
        // Keep the typed capability rather than round-tripping it through its
        // wire spelling: callers and tests match on `CapabilityFailure::capability`.
        let message = format!("command requires {cap}");
        Err(Error::MissingCapability(
            crate::error::CapabilityFailure::new(
                cap,
                crate::error::CapabilityFailureReason::NotAdvertised,
                message,
            ),
        ))
    }

    pub(super) fn require_enabled(&self, cap: Capability) -> Result<(), Error> {
        self.require(cap.clone())?;
        if self
            .snapshot
            .enabled
            .iter()
            .any(|enabled| enabled.eq_ignore_ascii_case(&cap.to_string()))
        {
            Ok(())
        } else {
            // An Outlook user expects negotiation before using a supported mode.
            // Advertising a mode without enabling it is caller sequencing, just
            // like UTF8=ONLY, rather than an unsupported server operation.
            Err(Error::InvalidState(LocalFailure::state(
                StateRequirement::Enabled(cap.clone()),
                format!("command requires ENABLE {cap} confirmation"),
            )))
        }
    }

    fn is_rev2(&self) -> bool {
        crate::types::profile::imap4rev2_active(&self.snapshot.capabilities, &self.snapshot.enabled)
    }
}

/// No bytes or pending effects are produced here. The same policy applies to
/// individual commands and every command in a pipeline, before its first write.
#[allow(clippy::too_many_lines)]
pub(super) fn admit(command: &Command, session: &SessionView<'_>) -> Result<(), Error> {
    let kind = command.kind();
    session.require_state(kind)?;
    if matches!(command, Command::StartTls) && session.tls_active {
        return Err(Error::InvalidState(LocalFailure::state(
            StateRequirement::Unencrypted,
            "STARTTLS requires an unencrypted connection",
        )));
    }
    let opts = encode_options(session.snapshot);
    // Criteria contribute requirements through the same literal-aware walk
    // for both snapshots and live state, including inside a pipeline.
    match command {
        Command::Search { criteria }
        | Command::UidSearch { criteria }
        | Command::SearchReturn { criteria, .. }
        | Command::UidSearchReturn { criteria, .. }
        | Command::SearchSave { criteria }
        | Command::UidSearchSave { criteria }
        | Command::Sort { criteria, .. }
        | Command::UidSort { criteria, .. }
        | Command::Thread { criteria, .. }
        | Command::UidThread { criteria, .. } => {
            crate::codec::encode::validate_search_criteria_crlf(
                criteria,
                "SEARCH criteria",
                opts.literal_mode,
            )?;
            for (atoms, capability) in [
                (&["MODSEQ"][..], Capability::Condstore),
                (&["OLDER", "YOUNGER"][..], Capability::Within),
                (
                    &["SAVEDBEFORE", "SAVEDON", "SAVEDSINCE", "SAVEDATESUPPORTED"][..],
                    Capability::SaveDate,
                ),
                (&["EMAILID", "THREADID"][..], Capability::ObjectId),
                (&["$"][..], Capability::SearchRes),
            ] {
                if atoms.iter().any(|atom| {
                    crate::connection::ImapConnection::search_criteria_contains_atom(criteria, atom)
                }) && !session.supports(&capability)
                {
                    return Err(crate::Error::MissingCapability(
                        capability.to_string().into(),
                    ));
                }
            }
        }
        _ => {}
    }
    match command {
        Command::StartTls => {
            // An Outlook user requesting encryption expects an unavailable
            // upgrade to fail as STARTTLS unavailable on every entry path.
            // An already encrypted session is sequencing, checked above.
            if !opts.has_capability(&Capability::StartTls) {
                return Err(Error::StartTlsUnavailable);
            }
        }
        Command::Enable { .. } => {
            // RFC 5161 Section 3.1: ENABLE requires ENABLE capability.
            if !opts.has_capability(&Capability::Enable) {
                return Err(missing("ENABLE", "ENABLE"));
            }
        }
        Command::Select { condstore, .. } => {
            // RFC 7162 Section 3.1.1: CONDSTORE requires CONDSTORE capability.
            if *condstore && !opts.has_condstore() {
                return Err(missing("SELECT (CONDSTORE)", "CONDSTORE"));
            }
        }
        Command::Examine { condstore, .. } => {
            // RFC 7162 Section 3.1.1: CONDSTORE requires CONDSTORE capability.
            if *condstore && !opts.has_condstore() {
                return Err(missing("EXAMINE (CONDSTORE)", "CONDSTORE"));
            }
        }
        Command::CreateSpecialUse { .. } => {
            // RFC 6154 Section 3: CREATE-SPECIAL-USE capability required.
            if !opts.has_capability(&Capability::CreateSpecialUse) {
                return Err(missing("CREATE (USE)", "CREATE-SPECIAL-USE"));
            }
        }
        Command::Unselect => {
            // RFC 3691 Section 2: UNSELECT requires UNSELECT capability.
            if !opts.has_capability(&Capability::Unselect) {
                return Err(missing("UNSELECT", "UNSELECT"));
            }
        }
        Command::Fetch { changed_since, .. } => {
            // RFC 7162 Section 3.1.4.1: CHANGEDSINCE requires CONDSTORE.
            if changed_since.is_some() && !opts.has_condstore() {
                return Err(missing("FETCH (CHANGEDSINCE)", "CONDSTORE"));
            }
        }
        Command::Store {
            unchanged_since, ..
        } => {
            // RFC 7162 Section 3.1.3: UNCHANGEDSINCE requires CONDSTORE.
            if unchanged_since.is_some() && !opts.has_condstore() {
                return Err(missing("STORE (UNCHANGEDSINCE)", "CONDSTORE"));
            }
        }
        Command::Move { .. } => {
            // RFC 6851 Section 3: MOVE requires MOVE capability.
            if !opts.has_capability(&Capability::Move) {
                return Err(missing("MOVE", "MOVE"));
            }
        }
        Command::UidFetch { changed_since, .. } => {
            // RFC 7162 Section 3.1.4.1: CHANGEDSINCE requires CONDSTORE.
            if changed_since.is_some() && !opts.has_condstore() {
                return Err(missing("UID FETCH (CHANGEDSINCE)", "CONDSTORE"));
            }
        }
        Command::UidStore {
            unchanged_since, ..
        } => {
            // RFC 7162 Section 3.1.3: UNCHANGEDSINCE requires CONDSTORE.
            if unchanged_since.is_some() && !opts.has_condstore() {
                return Err(missing("UID STORE (UNCHANGEDSINCE)", "CONDSTORE"));
            }
        }
        Command::UidMove { .. } => {
            // RFC 6851 Section 3: MOVE requires MOVE capability.
            if !opts.has_capability(&Capability::Move) {
                return Err(missing("UID MOVE", "MOVE"));
            }
        }
        Command::UidExpunge { .. } => {
            // RFC 4315: UID EXPUNGE requires UIDPLUS capability.
            if !opts.has_capability(&Capability::UidPlus) {
                return Err(missing("UID EXPUNGE", "UIDPLUS"));
            }
        }
        Command::Namespace => {
            if !opts.has_capability(&Capability::Namespace) {
                return Err(missing("NAMESPACE", "NAMESPACE"));
            }
        }
        Command::Idle => {
            // RFC 2177 Section 1: IDLE requires IDLE capability.
            if !opts.has_capability(&Capability::Idle) {
                return Err(missing("IDLE", "IDLE"));
            }
        }
        Command::Id(_) => {
            // RFC 2971 Section 2: ID requires ID capability.
            if !opts.has_capability(&Capability::Id) {
                return Err(missing("ID", "ID"));
            }
        }
        Command::GetMetadata { .. } => {
            // RFC 5464 Section 1: METADATA or METADATA-SERVER capability required.
            if !opts.has_capability(&Capability::Metadata)
                && !opts.has_capability(&Capability::MetadataServer)
            {
                return Err(missing("GETMETADATA", "METADATA"));
            }
        }
        Command::SetMetadata { .. } => {
            // RFC 5464 Section 1: METADATA or METADATA-SERVER capability required.
            if !opts.has_capability(&Capability::Metadata)
                && !opts.has_capability(&Capability::MetadataServer)
            {
                return Err(missing("SETMETADATA", "METADATA"));
            }
        }
        Command::Compress => {
            // RFC 4978 Section 4: COMPRESS DEFLATE requires COMPRESS=DEFLATE.
            if !opts.has_capability(&Capability::CompressDeflate) {
                return Err(missing("COMPRESS", "COMPRESS=DEFLATE"));
            }
        }
        Command::GetQuota { .. } => {
            // RFC 2087 Section 4.2: GETQUOTA requires QUOTA capability.
            if !opts.has_capability(&Capability::Quota) {
                return Err(missing("GETQUOTA", "QUOTA"));
            }
        }
        Command::GetQuotaRoot { .. } => {
            // RFC 2087 Section 4.3: GETQUOTAROOT requires QUOTA capability.
            if !opts.has_capability(&Capability::Quota) {
                return Err(missing("GETQUOTAROOT", "QUOTA"));
            }
        }
        Command::SetQuota { .. } => {
            // RFC 9208 Section 4.1.3: SETQUOTA requires QUOTASET capability.
            if !opts.has_capability(&Capability::QuotaSet) {
                return Err(missing("SETQUOTA", "QUOTASET"));
            }
        }
        Command::SetAcl { .. } => {
            // RFC 4314 Section 3.1: SETACL requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(missing("SETACL", "ACL"));
            }
        }
        Command::DeleteAcl { .. } => {
            // RFC 4314 Section 3.2: DELETEACL requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(missing("DELETEACL", "ACL"));
            }
        }
        Command::GetAcl { .. } => {
            // RFC 4314 Section 3.3: GETACL requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(missing("GETACL", "ACL"));
            }
        }
        Command::ListRights { .. } => {
            // RFC 4314 Section 3.4: LISTRIGHTS requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(missing("LISTRIGHTS", "ACL"));
            }
        }
        Command::MyRights { .. } => {
            // RFC 4314 Section 3.5: MYRIGHTS requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(missing("MYRIGHTS", "ACL"));
            }
        }
        Command::NotifySet(_) => {
            // RFC 5465 Section 3: NOTIFY requires NOTIFY capability.
            if !opts.has_capability(&Capability::Notify) {
                return Err(missing("NOTIFY SET", "NOTIFY"));
            }
        }
        Command::NotifyNone => {
            // RFC 5465 Section 3: NOTIFY requires NOTIFY capability.
            if !opts.has_capability(&Capability::Notify) {
                return Err(missing("NOTIFY NONE", "NOTIFY"));
            }
        }
        _ => {}
    }
    admit_arguments(command, session)?;
    Ok(())
}

pub(super) fn encode_options(snap: &ConnectionStateSnapshot) -> EncodeOptions {
    let rev2 = crate::types::profile::imap4rev2_active(&snap.capabilities, &snap.enabled);
    EncodeOptions {
        utf8_mode: rev2
            || snap
                .enabled
                .iter()
                .any(|e| e.eq_ignore_ascii_case("UTF8=ACCEPT")),
        literal_mode: if snap.capabilities.contains(&Capability::LiteralPlus) {
            crate::codec::encode::LiteralMode::LiteralPlus
        } else if crate::types::profile::supports(
            &snap.capabilities,
            &snap.enabled,
            &Capability::LiteralMinus,
        ) {
            crate::codec::encode::LiteralMode::LiteralMinus
        } else {
            crate::codec::encode::LiteralMode::Synchronizing
        },
        capabilities: snap.capabilities.clone(),
        enabled: snap.enabled.clone(),
    }
}

/// Proof that the handle snapshot admitted every command of one submission.
///
/// Only this module can mint it (the field is private), and
/// `ImapConnection::submit_driver_command` requires one. That makes the
/// driver's `admit_live` mapping sound by construction rather than by
/// convention: a refusal at the head of the queue can only be met by a command
/// the handle already admitted, so it is a state change before send, never a
/// permanent refusal the handle skipped.
pub(super) struct HandleAdmitted(());

impl ImapConnection {
    pub(super) fn admit_command(&self, command: &Command) -> Result<HandleAdmitted, Error> {
        let snap = self.state_rx.borrow();
        admit(
            command,
            &SessionView {
                snapshot: &snap,
                tls_active: self.is_encrypted(),
            },
        )?;
        Ok(HandleAdmitted(()))
    }

    /// Admit a whole pipeline against one handle snapshot.
    pub(super) fn admit_commands(&self, commands: &[Command]) -> Result<HandleAdmitted, Error> {
        let snap = self.state_rx.borrow();
        let session = SessionView {
            snapshot: &snap,
            tls_active: self.is_encrypted(),
        };
        for command in commands {
            admit(command, &session)?;
        }
        Ok(HandleAdmitted(()))
    }
}

impl SessionView<'_> {
    pub(super) fn check_utf8_only_enforced(&self) -> Result<(), Error> {
        let snap = self.snapshot;
        let utf8_enabled = snap
            .enabled
            .iter()
            .any(|e| e.eq_ignore_ascii_case("UTF8=ACCEPT"));
        let needs_utf8 =
            snap.capabilities.contains(&Capability::Utf8Only) && !utf8_enabled && !self.is_rev2();

        if needs_utf8 {
            // A mode the caller has not established, not a missing server
            // capability: the same request succeeds after the ENABLE.
            return Err(Error::InvalidState(crate::error::LocalFailure::state(
                crate::error::StateRequirement::Enabled(Capability::Utf8Accept),
                "server requires ENABLE UTF8=ACCEPT before use \
                 (UTF8=ONLY advertised, RFC 6855 Section 3)",
            )));
        }
        Ok(())
    }
    pub(super) fn require_state(&self, kind: crate::types::CommandKind) -> Result<(), Error> {
        let snap = self.snapshot;
        let session_state = snap.session_state;
        let mailbox_selected = snap.mailbox_selected;

        if let Some(refusal) = super::helpers::state_refusal(session_state, kind.legal_states()) {
            return Err(refusal);
        }
        if kind.refused_after_selection() && mailbox_selected {
            return Err(Error::InvalidState(crate::error::LocalFailure::state(
                crate::error::StateRequirement::BeforeFirstSelection { command: kind },
                format!(
                    "{kind:?} is not valid once a mailbox has been selected in the session \
                 (RFC 5161 Section 3.1)"
                ),
            )));
        }
        Ok(())
    }
    pub(super) fn require_condstore(&self) -> Result<(), Error> {
        let snap = self.snapshot;
        let has_condstore = crate::types::profile::supports(
            &snap.capabilities,
            &snap.enabled,
            &Capability::Condstore,
        );

        if !has_condstore {
            return Err(Error::MissingCapability("CONDSTORE".into()));
        }
        Ok(())
    }
    pub(super) fn require_searchres(&self) -> Result<(), Error> {
        let snap = self.snapshot;
        // RFC 9051 Appendix E: IMAP4rev2 folds SEARCHRES into the base protocol.
        let has_searchres = crate::types::profile::supports(
            &snap.capabilities,
            &snap.enabled,
            &Capability::SearchRes,
        );

        if !has_searchres {
            return Err(Error::MissingCapability("SEARCHRES".into()));
        }
        Ok(())
    }
    pub(super) fn has_quota_resource(&self, resource: &str) -> bool {
        let snap = self.snapshot;
        snap.capabilities.iter().any(|cap| {
            ImapConnection::quota_resource_name(cap)
                .is_some_and(|name| name.eq_ignore_ascii_case(resource))
        })
    }
    pub(super) fn validate_list_extended_request(
        &self,
        patterns: &[&str],
        selection_options: &[&str],
        return_options: &[&str],
    ) -> Result<(), Error> {
        if patterns.is_empty() {
            return Err(Error::InvalidInput(
                "LIST-EXTENDED requires at least one mailbox pattern \
                 (RFC 5258 Section 3 / RFC 9051 Section 6.3.9)"
                    .into(),
            ));
        }

        {
            let snap = self.snapshot;

            if patterns.len() > 1
                && !crate::types::profile::supports(
                    &snap.capabilities,
                    &snap.enabled,
                    &Capability::ListExtended,
                )
            {
                return Err(Error::MissingCapability("LIST-EXTENDED".into()));
            }

            // RFC 5258 Section 3 defines selection options and return options as
            // LIST-EXTENDED syntax on IMAP4rev1, even when the individual option
            // token comes from another extension such as RFC 6154 SPECIAL-USE or
            // RFC 5819 LIST-STATUS.
            let needs_list_extended = !selection_options.is_empty() || !return_options.is_empty();

            if needs_list_extended
                && !crate::types::profile::supports(
                    &snap.capabilities,
                    &snap.enabled,
                    &Capability::ListExtended,
                )
            {
                return Err(Error::MissingCapability("LIST-EXTENDED".into()));
            }

            for option in selection_options {
                let trimmed = option.trim();
                if trimmed.is_empty() {
                    return Err(Error::InvalidInput(
                        "LIST-EXTENDED selection options must not be empty \
                         (RFC 5258 Section 3 / RFC 9051 Section 6.3.9)"
                            .into(),
                    ));
                }
                if trimmed.eq_ignore_ascii_case("SPECIAL-USE")
                    && !crate::types::profile::supports(
                        &snap.capabilities,
                        &snap.enabled,
                        &Capability::SpecialUse,
                    )
                {
                    return Err(Error::MissingCapability("SPECIAL-USE".into()));
                }
            }
        }

        let has_recursivematch = selection_options
            .iter()
            .any(|option| option.trim().eq_ignore_ascii_case("RECURSIVEMATCH"));
        for option in return_options {
            let trimmed = option.trim();
            if trimmed.is_empty() {
                return Err(Error::InvalidInput(
                    "LIST-EXTENDED return options must not be empty \
                     (RFC 5258 Section 3 / RFC 9051 Section 6.3.9)"
                        .into(),
                ));
            }

            {
                let snap = self.snapshot;
                if trimmed.eq_ignore_ascii_case("SPECIAL-USE")
                    && !crate::types::profile::supports(
                        &snap.capabilities,
                        &snap.enabled,
                        &Capability::SpecialUse,
                    )
                {
                    return Err(Error::MissingCapability("SPECIAL-USE".into()));
                }
            }

            if let Some(status_items) =
                ImapConnection::list_status_return_option_items(trimmed).transpose()?
            {
                {
                    let snap = self.snapshot;
                    if !crate::types::profile::supports(
                        &snap.capabilities,
                        &snap.enabled,
                        &Capability::ListStatus,
                    ) {
                        return Err(Error::MissingCapability("LIST-STATUS".into()));
                    }
                }
                self.validate_requested_status_items(status_items)?;
            }
        }

        if has_recursivematch
            && !selection_options.iter().any(|option| {
                let trimmed = option.trim();
                !trimmed.is_empty()
                    && !trimmed.eq_ignore_ascii_case("RECURSIVEMATCH")
                    && !trimmed.eq_ignore_ascii_case("REMOTE")
            })
        {
            return Err(Error::InvalidInput(
                "LIST-EXTENDED selection option RECURSIVEMATCH requires another \
                 non-REMOTE selection option (RFC 5258 Section 3 / \
                 RFC 9051 Section 6.3.9)"
                    .into(),
            ));
        }

        Ok(())
    }
    pub(super) fn validate_requested_status_items(&self, items: &str) -> Result<(), Error> {
        for item in ImapConnection::status_item_tokens(items)? {
            match item.to_ascii_uppercase().as_str() {
                "RECENT" => {
                    if self.is_rev2() {
                        return Err(Error::MissingCapability(
                            crate::error::CapabilityFailure::new(
                                "IMAP4rev2".into(),
                                crate::error::CapabilityFailureReason::RemovedInRev2,
                                "STATUS item RECENT was removed in IMAP4rev2 \
                             (RFC 9051 Section 6.3.11)",
                            ),
                        ));
                    }
                }
                "DELETED" => {
                    // Deliberately NOT `snapshot_supports(StatusDeleted)`. The
                    // two agree under active rev2; they differ only on rev1,
                    // where the authority would also accept an advertised
                    // `STATUS=DELETED` token. No RFC defines that token: RFC
                    // 9051 Section 6.3.11 makes DELETED a base rev2 item, and
                    // the only rev1 route is RFC 9208 Section 4.1.4's
                    // QUOTA=RES-MESSAGE. The gate therefore names exactly
                    // those two sources.
                    if !self.is_rev2() && !self.has_quota_resource("MESSAGE") {
                        return Err(Error::MissingCapability(
                            crate::error::CapabilityFailure::new(
                                "QUOTA=RES-MESSAGE".into(),
                                crate::error::CapabilityFailureReason::Alternatives(vec![
                                    "IMAP4rev2".into(),
                                ]),
                                "STATUS item DELETED requires IMAP4rev2 or \
                             QUOTA=RES-MESSAGE (RFC 9051 Section 6.3.11 / \
                             RFC 9208 Section 4.1.4)",
                            ),
                        ));
                    }
                }
                "DELETED-STORAGE" => {
                    if !self.has_quota_resource("STORAGE") {
                        return Err(Error::MissingCapability("QUOTA=RES-STORAGE".into()));
                    }
                }
                "SIZE" => {
                    let snap = self.snapshot;
                    if !crate::types::profile::supports(
                        &snap.capabilities,
                        &snap.enabled,
                        &Capability::StatusSize,
                    ) {
                        return Err(Error::MissingCapability("STATUS=SIZE".into()));
                    }
                }
                "HIGHESTMODSEQ" => self.require_condstore()?,
                "APPENDLIMIT" => {
                    let has_appendlimit = !matches!(
                        crate::types::ServerProfile::new(
                            self.snapshot.capabilities.clone(),
                            self.snapshot.enabled.clone()
                        )
                        .append_limit,
                        crate::types::profile::AppendLimitPolicy::NotAdvertised
                    );
                    if !has_appendlimit {
                        return Err(Error::MissingCapability("APPENDLIMIT".into()));
                    }
                }
                "MAILBOXID" => {
                    let snap = self.snapshot;
                    if !crate::types::profile::supports(
                        &snap.capabilities,
                        &snap.enabled,
                        &Capability::ObjectId,
                    ) {
                        return Err(Error::MissingCapability("OBJECTID".into()));
                    }
                }
                _ => {}
            }
        }
        Ok(())
    }
}

#[allow(clippy::too_many_lines)]
fn admit_arguments(command: &Command, session: &SessionView<'_>) -> Result<(), Error> {
    // Mailbox and SEARCH arguments must use the negotiated UTF-8 mode. Mode
    // negotiation is sequencing: an Outlook user expects ENABLE before use,
    // never a permanent unsupported-operation answer for an available mode.
    if matches!(
        command,
        Command::List { .. }
            | Command::ListExtended { .. }
            | Command::ListStatus { .. }
            | Command::Lsub { .. }
            | Command::Select { .. }
            | Command::Examine { .. }
            | Command::Create { .. }
            | Command::CreateSpecialUse { .. }
            | Command::Delete { .. }
            | Command::Rename { .. }
            | Command::Subscribe { .. }
            | Command::Unsubscribe { .. }
            | Command::Append { .. }
            | Command::Status { .. }
            | Command::Copy { .. }
            | Command::UidCopy { .. }
            | Command::Move { .. }
            | Command::UidMove { .. }
            | Command::Search { .. }
            | Command::UidSearch { .. }
            | Command::SearchReturn { .. }
            | Command::UidSearchReturn { .. }
            | Command::SearchSave { .. }
            | Command::UidSearchSave { .. }
            | Command::Sort { .. }
            | Command::UidSort { .. }
            | Command::Thread { .. }
            | Command::UidThread { .. }
            | Command::GetMetadata { .. }
            | Command::SetMetadata { .. }
            | Command::GetQuotaRoot { .. }
            | Command::SetAcl { .. }
            | Command::DeleteAcl { .. }
            | Command::GetAcl { .. }
            | Command::ListRights { .. }
            | Command::MyRights { .. }
    ) {
        session.check_utf8_only_enforced()?;
    }

    match command {
        Command::Fetch {
            sequence_set,
            items,
            ..
        }
        | Command::UidFetch {
            sequence_set,
            items,
            ..
        } => {
            session.validate_sequence_set(sequence_set)?;
            session.validate_fetch_wire(items)?;
        }
        Command::Store { sequence_set, .. }
        | Command::UidStore { sequence_set, .. }
        | Command::Copy { sequence_set, .. }
        | Command::UidCopy { sequence_set, .. }
        | Command::Move { sequence_set, .. }
        | Command::UidMove { sequence_set, .. }
        | Command::UidExpunge { sequence_set } => session.validate_sequence_set(sequence_set)?,
        _ => {}
    }
    match command {
        Command::Select {
            qresync: Some(params),
            ..
        }
        | Command::Examine {
            qresync: Some(params),
            ..
        } => {
            session.validate_qresync_params(params)?;
        }
        Command::UidFetch {
            vanished: true,
            changed_since,
            ..
        } => {
            session.require_enabled(Capability::QResync)?;
            if changed_since.is_none() {
                return Err(Error::InvalidInput("VANISHED requires CHANGEDSINCE".into()));
            }
        }
        Command::Login { .. } if session.supports(&Capability::LoginDisabled) => {
            return Err(Error::MissingCapability(
                crate::error::CapabilityFailure::new(
                    Capability::LoginDisabled,
                    crate::error::CapabilityFailureReason::ProhibitedByServer,
                    "LOGIN disabled by server (LOGINDISABLED)",
                ),
            ));
        }
        Command::Authenticate {
            mechanism,
            initial_response,
        } => {
            session.require(Capability::from_imap_str(&format!("AUTH={mechanism}")))?;
            if initial_response.is_some() {
                session.require(Capability::SaslIr)?;
            }
        }
        Command::Unauthenticate => session.require(Capability::Unauthenticate)?,
        Command::SearchReturn { .. } | Command::UidSearchReturn { .. } => {
            session.require(Capability::Esearch)?;
            if ImapConnection::search_return_requests_save(command) {
                session.require(Capability::SearchRes)?;
            }
        }
        Command::SearchSave { .. } | Command::UidSearchSave { .. } => {
            session.require(Capability::SearchRes)?;
        }
        Command::Sort { .. } | Command::UidSort { .. } => session.require(Capability::Sort)?,
        Command::Thread { algorithm, .. } | Command::UidThread { algorithm, .. } => {
            session.require(Capability::from_imap_str(&format!("THREAD={algorithm}")))?;
        }
        Command::Status { items, .. } => session.validate_requested_status_items(items)?,
        Command::ListStatus { status_items, .. } => {
            session.require(Capability::ListExtended)?;
            session.require(Capability::ListStatus)?;
            session.validate_requested_status_items(status_items)?;
        }
        Command::ListExtended {
            patterns,
            selection_options,
            return_options,
            ..
        } => {
            // Only the pattern count enters admission; their wire identities
            // remain typed until encoding.
            let patterns = vec![""; patterns.len()];
            let selection = selection_options
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            let returns = return_options
                .iter()
                .map(String::as_str)
                .collect::<Vec<_>>();
            session.validate_list_extended_request(&patterns, &selection, &returns)?;
        }
        Command::NotifySet(params) => {
            if params.event_groups.iter().any(|group| {
                matches!(
                    &group.filter,
                    crate::types::MailboxFilter::Mailboxes(_)
                        | crate::types::MailboxFilter::Subtree(_)
                )
            }) {
                session.check_utf8_only_enforced()?;
            }
        }
        Command::Append {
            messages, multi, ..
        } => {
            if *multi {
                session.require(Capability::MultiAppend)?;
            }
            let wrapper = session
                .snapshot
                .enabled
                .iter()
                .any(|e| e.eq_ignore_ascii_case("UTF8=ACCEPT"));
            if !wrapper
                && messages.iter().any(|msg| msg.data.contains(&0))
                && !session.supports(&Capability::Binary)
            {
                return Err(Error::MissingCapability(
                    crate::error::CapabilityFailure::new(
                        Capability::Binary,
                        crate::error::CapabilityFailureReason::NotAdvertised,
                        "APPEND data containing NUL requires BINARY literal8 support",
                    ),
                ));
            }
        }
        Command::Check | Command::Lsub { .. } if session.is_rev2() => {
            return Err(Error::MissingCapability(
                crate::error::CapabilityFailure::new(
                    "IMAP4rev2".into(),
                    crate::error::CapabilityFailureReason::RemovedInRev2,
                    format!("{:?} is not available in IMAP4rev2", command.kind()),
                ),
            ));
        }
        _ => {}
    }
    Ok(())
}

impl SessionView<'_> {
    pub(super) fn validate_qresync_params(
        &self,
        params: &crate::types::QresyncParams,
    ) -> Result<(), Error> {
        self.require_enabled(Capability::QResync)?;
        if params.seq_match_data.is_some() && params.known_uids.is_none() {
            return Err(Error::InvalidInput(
                "QRESYNC seq-match-data requires known-uids".into(),
            ));
        }
        Ok(())
    }

    fn validate_sequence_set(&self, sequence_set: &crate::types::SequenceSet) -> Result<(), Error> {
        // Saved-result references must mean the same thing in a pipeline as in
        // a single request. Outlook users expect the same mailbox operation.
        if sequence_set.as_str().contains('$') {
            self.require(Capability::SearchRes)?;
        }
        Ok(())
    }

    pub(super) fn validate_fetch_wire(&self, items: &str) -> Result<(), Error> {
        // Walk top-level FETCH item names only. Header field names inside a
        // BODY section are data, even when named MODSEQ, PREVIEW or SAVEDATE.
        // Both typed helpers and the raw pipeline builder enter this walk.
        let mut section_depth = 0usize;
        let mut quoted = false;
        let mut escaped = false;
        for atom in items.split(|c: char| c.is_ascii_whitespace() || c == '(' || c == ')') {
            let name = atom.split('[').next().unwrap_or("");
            if section_depth == 0 && !quoted {
                let cap = match name.to_ascii_uppercase().as_str() {
                    "MODSEQ" => Some(Capability::Condstore),
                    "PREVIEW" => Some(Capability::Preview),
                    "SAVEDATE" => Some(Capability::SaveDate),
                    "EMAILID" | "THREADID" => Some(Capability::ObjectId),
                    "X-GM-MSGID" | "X-GM-THRID" | "X-GM-LABELS" => Some(Capability::XGmExt1),
                    "BINARY" | "BINARY.PEEK" | "BINARY.SIZE" => {
                        if !crate::types::profile::binary_fetch_usable(
                            &self.snapshot.capabilities,
                            &self.snapshot.enabled,
                        ) {
                            return Err(missing("FETCH", "BINARY"));
                        }
                        None
                    }
                    _ => None,
                };
                if let Some(cap) = cap {
                    self.require(cap)?;
                }
            }
            for byte in atom.bytes() {
                if escaped {
                    escaped = false;
                    continue;
                }
                if quoted && byte == b'\\' {
                    escaped = true;
                    continue;
                }
                if byte == b'"' {
                    quoted = !quoted;
                    continue;
                }
                if !quoted {
                    if byte == b'[' {
                        section_depth += 1;
                    }
                    if byte == b']' {
                        section_depth = section_depth.saturating_sub(1);
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    #![allow(clippy::unwrap_used, clippy::expect_used)]
    use super::super::SessionState;
    use super::*;
    use crate::types::{QresyncParams, SequenceSet};

    fn snapshot(capabilities: Vec<Capability>, enabled: &[&str]) -> ConnectionStateSnapshot {
        ConnectionStateSnapshot {
            session_state: SessionState::Selected,
            capabilities,
            enabled: enabled.iter().map(|value| (*value).to_owned()).collect(),
            mailbox_selected: true,
            selected: None,
        }
    }

    fn fetch(set: &str, items: &str) -> Command {
        Command::UidFetch {
            sequence_set: SequenceSet::new(set).unwrap(),
            items: items.into(),
            changed_since: None,
            vanished: false,
        }
    }

    fn check(cmd: &Command, snap: &ConnectionStateSnapshot) -> Result<(), Error> {
        admit(
            cmd,
            &SessionView {
                snapshot: snap,
                tls_active: false,
            },
        )
    }

    #[test]
    fn raw_fetch_and_saved_sets_have_the_typed_handles_gates() {
        let base = snapshot(vec![Capability::Imap4Rev1], &[]);
        for (cmd, cap) in [
            (fetch("$", "UID"), Capability::SearchRes),
            (fetch("1", "MODSEQ"), Capability::Condstore),
            (fetch("1", "PREVIEW (LAZY)"), Capability::Preview),
            (fetch("1", "SAVEDATE"), Capability::SaveDate),
            (fetch("1", "EMAILID THREADID"), Capability::ObjectId),
            (fetch("1", "X-GM-LABELS"), Capability::XGmExt1),
            (fetch("1", "BINARY.PEEK[]"), Capability::Binary),
        ] {
            assert!(
                matches!(check(&cmd, &base), Err(Error::MissingCapability(_))),
                "{cmd:?}"
            );
            let mut available = base.clone();
            available.capabilities.push(cap);
            assert!(check(&cmd, &available).is_ok(), "{cmd:?}");
        }
        // Header names are data, never requirements of the FETCH command.
        assert!(
            check(
                &fetch("1", "BODY.PEEK[HEADER.FIELDS (MODSEQ PREVIEW SAVEDATE)]"),
                &base
            )
            .is_ok()
        );
        assert!(
            check(
                &fetch("$", "BINARY[]"),
                &snapshot(vec![Capability::Imap4Rev2], &[])
            )
            .is_ok()
        );
    }

    #[test]
    fn qresync_requests_require_confirmation_even_on_the_live_path() {
        let mut snap = snapshot(vec![Capability::QResync], &[]);
        let select = Command::Select {
            mailbox: crate::types::MailboxName::new("INBOX").unwrap(),
            condstore: false,
            qresync: Some(QresyncParams {
                uid_validity: 1,
                mod_seq: 2,
                known_uids: None,
                seq_match_data: None,
            }),
        };
        let vanished = Command::UidFetch {
            sequence_set: SequenceSet::new("1").unwrap(),
            items: "UID".into(),
            changed_since: Some(2),
            vanished: true,
        };
        for cmd in [&select, &vanished] {
            assert!(matches!(check(cmd, &snap), Err(Error::InvalidState(_))));
        }
        snap.enabled.push("Qresync".into());
        for cmd in [&select, &vanished] {
            assert!(check(cmd, &snap).is_ok());
        }
    }

    #[test]
    fn utf8_only_is_checked_at_admission_for_pipeline_mailboxes() {
        let mut snap = snapshot(vec![Capability::Utf8Only, Capability::Utf8Accept], &[]);
        let cmd = Command::Copy {
            sequence_set: SequenceSet::new("1").unwrap(),
            mailbox: crate::types::MailboxName::new("Archive").unwrap(),
        };
        assert!(matches!(check(&cmd, &snap), Err(Error::InvalidState(_))));
        snap.enabled.push("UTF8=ACCEPT".into());
        assert!(check(&cmd, &snap).is_ok());
    }

    #[test]
    fn starttls_uses_one_availability_and_transport_policy() {
        let mut snap = snapshot(vec![], &[]);
        snap.session_state = SessionState::NotAuthenticated;
        snap.mailbox_selected = false;
        assert_eq!(
            check(&Command::StartTls, &snap),
            Err(Error::StartTlsUnavailable)
        );
        snap.capabilities.push(Capability::StartTls);
        assert!(check(&Command::StartTls, &snap).is_ok());
        assert!(matches!(
            admit(
                &Command::StartTls,
                &SessionView {
                    snapshot: &snap,
                    tls_active: true
                }
            ),
            Err(Error::InvalidState(_))
        ));
    }

    #[test]
    fn queued_capability_loss_is_a_state_change_before_send() {
        let mut state = super::super::state::ProtocolState::new();
        state
            .apply_greeting(&crate::types::response::GreetingResponse {
                status: crate::types::response::GreetingStatus::PreAuth,
                code: None,
                text: "ready".into(),
            })
            .unwrap();
        let cmd = Command::Namespace;
        state.apply_capability_fetch(vec![Capability::Namespace]);
        assert!(super::super::driver::admit_live(&state, &cmd).is_ok());
        state.apply_capability_fetch(Vec::new());
        let error = super::super::driver::admit_live(&state, &cmd).unwrap_err();
        assert!(matches!(&error, Error::StateChangedBeforeSend(_)));
        assert_eq!(
            error.attempt(),
            Some(bifrost_types::TransmissionState::Unsent)
        );
    }

    #[test]
    fn literal_free_coalescing_preserves_chunks_and_refuses_sync_boundaries() {
        let snap = snapshot(vec![], &[]);
        let opts = encode_options(&snap);
        let first = crate::codec::encode::encode_command("A1", &Command::Noop, &opts).unwrap();
        let second =
            crate::codec::encode::encode_command("A2", &Command::Capability, &opts).unwrap();
        let combined =
            crate::codec::encode::WireCommand::coalesce_unsegmented(&[first, second]).unwrap();
        assert_eq!(combined.to_vec(), b"A1 NOOP\r\nA2 CAPABILITY\r\n");
        let mut writer = crate::codec::encode::CommandWriter::new(&opts);
        writer.raw(b"A3 X ");
        writer.literal(b"payload", crate::codec::encode::LiteralForm::Classic);
        writer.raw(b"\r\n");
        assert!(
            crate::codec::encode::WireCommand::coalesce_unsegmented(&[writer.finish().unwrap()])
                .is_none()
        );
    }
}
