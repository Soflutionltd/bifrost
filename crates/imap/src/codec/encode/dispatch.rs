use crate::types::Command;
use crate::types::response::Capability;

use super::commands::{
    encode_append, encode_authenticate, encode_create_special_use, encode_fetch,
    encode_getmetadata, encode_id, encode_list_extended, encode_list_status, encode_login,
    encode_mailbox_cmd, encode_mailbox_name, encode_mailbox_str, encode_notify_set, encode_search,
    encode_select_or_examine, encode_set_acl, encode_set_quota, encode_setmetadata, encode_simple,
    encode_status, encode_store, encode_thread_or_sort_cmd, encode_two_arg, encode_two_quoted_args,
    encode_uid_expunge, encode_uid_fetch,
};
use super::{CommandWriter, EncodeOptions, WireCommand, encode_enable, validate_atom};

/// The refusal for a command whose prerequisite capability is not usable on
/// this connection (I6). One wording for every command: `<cmd> requires <cap>`.
fn missing(cmd: &str, cap: &str) -> crate::Error {
    crate::Error::MissingCapability(format!("{cmd} requires {cap}"))
}

/// Encodes an IMAP command into a [`WireCommand`].
///
/// The single encoder for every command, APPEND and MULTIAPPEND included.
/// The driver calls it at send time with [`EncodeOptions`] built from the
/// state it owns, so literal markers (RFC 7888), literal8 eligibility, the
/// mailbox encoding (modified UTF-7 or UTF-8, RFC 6855 Section 3) and every
/// capability gate reflect the connection as it is when the command reaches
/// the head of the queue.
///
/// Capability prerequisite checks (I6) run before any byte is produced:
/// commands that require capabilities not usable per `opts` return
/// [`crate::Error::MissingCapability`]. Every failure here is a refusal before
/// the first byte, so the connection stays usable.
///
/// Tag-command format per RFC 3501 Section 2.2.1 / RFC 9051 Section 2.2.1.
#[allow(clippy::too_many_lines)]
pub(crate) fn encode_command(
    tag: &str,
    command: &Command,
    opts: &EncodeOptions,
) -> Result<WireCommand, crate::Error> {
    // Check criteria at encoding: a queued command's handle snapshot may
    // predate a CAPABILITY/ENABLE update, including inside a pipeline.
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
                }) && !opts.has_capability(&capability)
                {
                    return Err(crate::Error::MissingCapability(capability.to_string()));
                }
            }
        }
        _ => {}
    }
    let mut w = CommandWriter::new(opts);
    let utf8 = opts.utf8_mode;
    match command {
        Command::Login { user, pass } => {
            encode_login(&mut w, tag, user, pass, utf8)?;
        }
        Command::Authenticate {
            mechanism,
            initial_response,
        } => {
            // RFC 3501 Section 6.2.2: auth-type = atom
            validate_atom(mechanism, "AUTHENTICATE mechanism")?;
            encode_authenticate(&mut w, tag, mechanism, initial_response.as_deref())?;
        }
        // RFC 3501 Section 6.2.1 / RFC 9051 Section 6.2.1
        Command::StartTls => {
            if !opts.has_capability(&Capability::StartTls) {
                return Err(missing("STARTTLS", "STARTTLS"));
            }
            encode_simple(&mut w, tag, "STARTTLS");
        }
        // RFC 3501 Section 6.1.3 / RFC 9051 Section 6.1.3
        Command::Logout => {
            encode_simple(&mut w, tag, "LOGOUT");
        }
        // RFC 5161 Section 3
        Command::Enable { capabilities } => {
            // RFC 5161 Section 3.1: ENABLE requires ENABLE capability.
            if !opts.has_capability(&Capability::Enable) {
                return Err(missing("ENABLE", "ENABLE"));
            }
            encode_enable(&mut w, tag, capabilities)?;
        }
        Command::List { reference, pattern } => {
            let wire_ref = encode_mailbox_str(reference, utf8);
            let wire_pat = encode_mailbox_str(pattern, utf8);
            encode_two_quoted_args(&mut w, tag, "LIST", &wire_ref, &wire_pat, utf8);
        }
        Command::ListExtended {
            selection_options,
            reference,
            patterns,
            return_options,
        } => {
            encode_list_extended(
                &mut w,
                tag,
                selection_options,
                reference,
                patterns,
                return_options,
                utf8,
            )?;
        }
        Command::ListStatus {
            reference,
            pattern,
            status_items,
        } => {
            encode_list_status(&mut w, tag, reference, pattern, status_items, utf8)?;
        }
        Command::Select {
            mailbox,
            condstore,
            qresync,
        } => {
            // RFC 7162 Section 3.1.1: CONDSTORE requires CONDSTORE capability.
            if *condstore && !opts.has_condstore() {
                return Err(missing("SELECT (CONDSTORE)", "CONDSTORE"));
            }
            // RFC 7162 Section 3.2.5.2: QRESYNC requires QRESYNC capability.
            if qresync.is_some() && !opts.has_capability(&Capability::QResync) {
                return Err(missing("SELECT (QRESYNC)", "QRESYNC"));
            }
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_select_or_examine(
                &mut w,
                tag,
                "SELECT",
                &wire,
                *condstore,
                qresync.as_ref(),
                utf8,
            )?;
        }
        Command::Examine {
            mailbox,
            condstore,
            qresync,
        } => {
            // RFC 7162 Section 3.1.1: CONDSTORE requires CONDSTORE capability.
            if *condstore && !opts.has_condstore() {
                return Err(missing("EXAMINE (CONDSTORE)", "CONDSTORE"));
            }
            // RFC 7162 Section 3.2.5.2: QRESYNC requires QRESYNC capability.
            if qresync.is_some() && !opts.has_capability(&Capability::QResync) {
                return Err(missing("EXAMINE (QRESYNC)", "QRESYNC"));
            }
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_select_or_examine(
                &mut w,
                tag,
                "EXAMINE",
                &wire,
                *condstore,
                qresync.as_ref(),
                utf8,
            )?;
        }
        Command::Create { mailbox } => {
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_mailbox_cmd(&mut w, tag, "CREATE", &wire, utf8);
        }
        Command::CreateSpecialUse {
            mailbox,
            special_use,
        } => {
            // RFC 6154 Section 3: CREATE-SPECIAL-USE capability required.
            if !opts.has_capability(&Capability::CreateSpecialUse) {
                return Err(missing("CREATE (USE)", "CREATE-SPECIAL-USE"));
            }
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_create_special_use(&mut w, tag, &wire, special_use, utf8);
        }
        Command::Delete { mailbox } => {
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_mailbox_cmd(&mut w, tag, "DELETE", &wire, utf8);
        }
        Command::Rename { mailbox, new_name } => {
            let wire_old = encode_mailbox_name(mailbox, utf8)?;
            let wire_new = encode_mailbox_name(new_name, utf8)?;
            encode_two_quoted_args(&mut w, tag, "RENAME", &wire_old, &wire_new, utf8);
        }
        Command::Subscribe { mailbox } => {
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_mailbox_cmd(&mut w, tag, "SUBSCRIBE", &wire, utf8);
        }
        Command::Unsubscribe { mailbox } => {
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_mailbox_cmd(&mut w, tag, "UNSUBSCRIBE", &wire, utf8);
        }
        Command::Lsub { reference, pattern } => {
            let wire_ref = encode_mailbox_str(reference, utf8);
            let wire_pat = encode_mailbox_str(pattern, utf8);
            encode_two_quoted_args(&mut w, tag, "LSUB", &wire_ref, &wire_pat, utf8);
        }
        // RFC 3501 Section 6.3.11 / RFC 3502: APPEND and MULTIAPPEND carry
        // their own validation (MULTIAPPEND, BINARY for a NUL body outside
        // the RFC 6855 wrapper, flags, dates), all against these live options.
        Command::Append {
            mailbox,
            messages,
            multi,
        } => {
            encode_append(&mut w, tag, mailbox, messages, *multi, opts)?;
        }
        // RFC 3501 Section 6.4.2 / RFC 9051 Section 6.4.1
        Command::Close => {
            encode_simple(&mut w, tag, "CLOSE");
        }
        // RFC 8437 Section 2
        Command::Unauthenticate => {
            encode_simple(&mut w, tag, "UNAUTHENTICATE");
        }
        // RFC 3691 Section 3
        Command::Unselect => {
            // RFC 3691 Section 2: UNSELECT requires UNSELECT capability.
            if !opts.has_capability(&Capability::Unselect) {
                return Err(missing("UNSELECT", "UNSELECT"));
            }
            encode_simple(&mut w, tag, "UNSELECT");
        }
        Command::Status { mailbox, items } => {
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_status(&mut w, tag, &wire, items, utf8)?;
        }
        Command::Search { criteria } => {
            encode_search(&mut w, tag, "SEARCH", criteria, None, utf8)?;
        }
        Command::SearchReturn {
            criteria,
            return_opts,
        } => {
            encode_search(&mut w, tag, "SEARCH", criteria, Some(return_opts), utf8)?;
        }
        Command::SearchSave { criteria } => {
            encode_search(&mut w, tag, "SEARCH RETURN (SAVE)", criteria, None, utf8)?;
        }
        Command::Fetch {
            sequence_set,
            items,
            changed_since,
        } => {
            // RFC 7162 Section 3.1.4.1: CHANGEDSINCE requires CONDSTORE.
            if changed_since.is_some() && !opts.has_condstore() {
                return Err(missing("FETCH (CHANGEDSINCE)", "CONDSTORE"));
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_fetch(&mut w, tag, sequence_set.as_str(), items, *changed_since)?;
        }
        Command::Store {
            sequence_set,
            operation,
            flags,
            unchanged_since,
        } => {
            // RFC 7162 Section 3.1.3: UNCHANGEDSINCE requires CONDSTORE.
            if unchanged_since.is_some() && !opts.has_condstore() {
                return Err(missing("STORE (UNCHANGEDSINCE)", "CONDSTORE"));
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_store(
                &mut w,
                tag,
                false,
                sequence_set.as_str(),
                *operation,
                flags,
                *unchanged_since,
            )?;
        }
        Command::Copy {
            sequence_set,
            mailbox,
        } => {
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_two_arg(
                &mut w,
                tag,
                false,
                "COPY",
                sequence_set.as_str(),
                &wire,
                utf8,
            )?;
        }
        Command::Move {
            sequence_set,
            mailbox,
        } => {
            // RFC 6851 Section 3: MOVE requires MOVE capability.
            if !opts.has_capability(&Capability::Move) {
                return Err(missing("MOVE", "MOVE"));
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_two_arg(
                &mut w,
                tag,
                false,
                "MOVE",
                sequence_set.as_str(),
                &wire,
                utf8,
            )?;
        }
        Command::UidSearch { criteria } => {
            encode_search(&mut w, tag, "UID SEARCH", criteria, None, utf8)?;
        }
        Command::UidSearchReturn {
            criteria,
            return_opts,
        } => {
            encode_search(&mut w, tag, "UID SEARCH", criteria, Some(return_opts), utf8)?;
        }
        Command::UidSearchSave { criteria } => {
            encode_search(
                &mut w,
                tag,
                "UID SEARCH RETURN (SAVE)",
                criteria,
                None,
                utf8,
            )?;
        }
        Command::UidFetch {
            sequence_set,
            items,
            changed_since,
            vanished,
        } => {
            // RFC 7162 Section 3.1.4.1: CHANGEDSINCE requires CONDSTORE.
            if changed_since.is_some() && !opts.has_condstore() {
                return Err(missing("UID FETCH (CHANGEDSINCE)", "CONDSTORE"));
            }
            // RFC 7162 Section 3.2.6: VANISHED requires QRESYNC.
            if *vanished && !opts.has_capability(&Capability::QResync) {
                return Err(missing("UID FETCH (VANISHED)", "QRESYNC"));
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_uid_fetch(
                &mut w,
                tag,
                sequence_set.as_str(),
                items,
                *changed_since,
                *vanished,
            )?;
        }
        Command::UidStore {
            sequence_set,
            operation,
            flags,
            unchanged_since,
        } => {
            // RFC 7162 Section 3.1.3: UNCHANGEDSINCE requires CONDSTORE.
            if unchanged_since.is_some() && !opts.has_condstore() {
                return Err(missing("UID STORE (UNCHANGEDSINCE)", "CONDSTORE"));
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_store(
                &mut w,
                tag,
                true,
                sequence_set.as_str(),
                *operation,
                flags,
                *unchanged_since,
            )?;
        }
        Command::UidMove {
            sequence_set,
            mailbox,
        } => {
            // RFC 6851 Section 3: MOVE requires MOVE capability.
            if !opts.has_capability(&Capability::Move) {
                return Err(missing("UID MOVE", "MOVE"));
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_two_arg(
                &mut w,
                tag,
                true,
                "MOVE",
                sequence_set.as_str(),
                &wire,
                utf8,
            )?;
        }
        Command::UidCopy {
            sequence_set,
            mailbox,
        } => {
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_two_arg(
                &mut w,
                tag,
                true,
                "COPY",
                sequence_set.as_str(),
                &wire,
                utf8,
            )?;
        }
        Command::UidExpunge { sequence_set } => {
            // RFC 4315: UID EXPUNGE requires UIDPLUS capability.
            if !opts.has_capability(&Capability::UidPlus) {
                return Err(missing("UID EXPUNGE", "UIDPLUS"));
            }
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_uid_expunge(&mut w, tag, sequence_set.as_str())?;
        }
        // RFC 2342 Section 5 / RFC 9051 Section 6.3.10
        Command::Namespace => {
            if !opts.has_capability(&Capability::Namespace) {
                return Err(missing("NAMESPACE", "NAMESPACE"));
            }
            encode_simple(&mut w, tag, "NAMESPACE");
        }
        // RFC 3501 Section 6.4.1 (removed in IMAP4rev2)
        Command::Check => {
            encode_simple(&mut w, tag, "CHECK");
        }
        // RFC 3501 Section 6.4.3 / RFC 9051 Section 6.4.3
        Command::Expunge => {
            encode_simple(&mut w, tag, "EXPUNGE");
        }
        // RFC 2177 Section 3 / RFC 9051 Section 6.3.13
        Command::Idle => {
            // RFC 2177 Section 1: IDLE requires IDLE capability.
            if !opts.has_capability(&Capability::Idle) {
                return Err(missing("IDLE", "IDLE"));
            }
            encode_simple(&mut w, tag, "IDLE");
        }
        // RFC 3501 Section 6.1.1 / RFC 9051 Section 6.1.1
        Command::Capability => {
            encode_simple(&mut w, tag, "CAPABILITY");
        }
        // RFC 3501 Section 6.1.2 / RFC 9051 Section 6.1.2
        Command::Noop => {
            encode_simple(&mut w, tag, "NOOP");
        }
        Command::Id(params) => {
            // RFC 2971 Section 2: ID requires ID capability.
            if !opts.has_capability(&Capability::Id) {
                return Err(missing("ID", "ID"));
            }
            encode_id(&mut w, tag, params, utf8)?;
        }
        Command::GetMetadata {
            mailbox,
            entries,
            max_size,
            depth,
        } => {
            // RFC 5464 Section 1: METADATA or METADATA-SERVER capability required.
            if !opts.has_capability(&Capability::Metadata)
                && !opts.has_capability(&Capability::MetadataServer)
            {
                return Err(missing("GETMETADATA", "METADATA"));
            }
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_getmetadata(
                &mut w,
                tag,
                &wire,
                entries,
                *max_size,
                depth.as_deref(),
                utf8,
            )?;
        }
        Command::SetMetadata { mailbox, entries } => {
            // RFC 5464 Section 1: METADATA or METADATA-SERVER capability required.
            if !opts.has_capability(&Capability::Metadata)
                && !opts.has_capability(&Capability::MetadataServer)
            {
                return Err(missing("SETMETADATA", "METADATA"));
            }
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_setmetadata(&mut w, tag, &wire, entries, utf8)?;
        }
        Command::Thread {
            algorithm,
            charset,
            criteria,
        } => {
            encode_thread_or_sort_cmd(&mut w, tag, "THREAD", algorithm, charset, criteria, false)?;
        }
        Command::UidThread {
            algorithm,
            charset,
            criteria,
        } => {
            encode_thread_or_sort_cmd(
                &mut w,
                tag,
                "UID THREAD",
                algorithm,
                charset,
                criteria,
                false,
            )?;
        }
        Command::Sort {
            sort_criteria,
            charset,
            criteria,
        } => {
            encode_thread_or_sort_cmd(&mut w, tag, "SORT", sort_criteria, charset, criteria, true)?;
        }
        Command::UidSort {
            sort_criteria,
            charset,
            criteria,
        } => {
            encode_thread_or_sort_cmd(
                &mut w,
                tag,
                "UID SORT",
                sort_criteria,
                charset,
                criteria,
                true,
            )?;
        }
        Command::Compress => {
            // RFC 4978 Section 4: COMPRESS DEFLATE requires COMPRESS=DEFLATE.
            if !opts.has_capability(&Capability::CompressDeflate) {
                return Err(missing("COMPRESS", "COMPRESS=DEFLATE"));
            }
            w.raw(tag.as_bytes());
            w.raw(b" COMPRESS DEFLATE\r\n");
        }

        // --- QUOTA (RFC 2087) ---
        Command::GetQuota { root } => {
            // RFC 2087 Section 4.2: GETQUOTA requires QUOTA capability.
            if !opts.has_capability(&Capability::Quota) {
                return Err(missing("GETQUOTA", "QUOTA"));
            }
            encode_mailbox_cmd(&mut w, tag, "GETQUOTA", root, utf8);
        }
        Command::GetQuotaRoot { mailbox } => {
            // RFC 2087 Section 4.3: GETQUOTAROOT requires QUOTA capability.
            if !opts.has_capability(&Capability::Quota) {
                return Err(missing("GETQUOTAROOT", "QUOTA"));
            }
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_mailbox_cmd(&mut w, tag, "GETQUOTAROOT", &wire, utf8);
        }
        Command::SetQuota { root, resources } => {
            // RFC 9208 Section 4.1.3: SETQUOTA requires QUOTASET capability.
            if !opts.has_capability(&Capability::QuotaSet) {
                return Err(missing("SETQUOTA", "QUOTASET"));
            }
            encode_set_quota(&mut w, tag, root, resources, utf8)?;
        }

        // --- ACL (RFC 4314) ---
        Command::SetAcl {
            mailbox,
            identifier,
            rights,
        } => {
            // RFC 4314 Section 3.1: SETACL requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(missing("SETACL", "ACL"));
            }
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_set_acl(&mut w, tag, &wire, identifier, rights, utf8);
        }
        Command::DeleteAcl {
            mailbox,
            identifier,
        } => {
            // RFC 4314 Section 3.2: DELETEACL requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(missing("DELETEACL", "ACL"));
            }
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_two_quoted_args(&mut w, tag, "DELETEACL", &wire, identifier, utf8);
        }
        Command::GetAcl { mailbox } => {
            // RFC 4314 Section 3.3: GETACL requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(missing("GETACL", "ACL"));
            }
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_mailbox_cmd(&mut w, tag, "GETACL", &wire, utf8);
        }
        Command::ListRights {
            mailbox,
            identifier,
        } => {
            // RFC 4314 Section 3.4: LISTRIGHTS requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(missing("LISTRIGHTS", "ACL"));
            }
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_two_quoted_args(&mut w, tag, "LISTRIGHTS", &wire, identifier, utf8);
        }
        Command::MyRights { mailbox } => {
            // RFC 4314 Section 3.5: MYRIGHTS requires ACL capability.
            if !opts.has_capability(&Capability::Acl) {
                return Err(missing("MYRIGHTS", "ACL"));
            }
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_mailbox_cmd(&mut w, tag, "MYRIGHTS", &wire, utf8);
        }

        // --- NOTIFY (RFC 5465) ---
        Command::NotifySet(params) => {
            // RFC 5465 Section 3: NOTIFY requires NOTIFY capability.
            if !opts.has_capability(&Capability::Notify) {
                return Err(missing("NOTIFY SET", "NOTIFY"));
            }
            encode_notify_set(&mut w, tag, params, utf8)?;
        }
        Command::NotifyNone => {
            // RFC 5465 Section 3: NOTIFY requires NOTIFY capability.
            if !opts.has_capability(&Capability::Notify) {
                return Err(missing("NOTIFY NONE", "NOTIFY"));
            }
            encode_simple(&mut w, tag, "NOTIFY NONE");
        }
    }
    Ok(w.finish())
}

/// Encode `command` and append the whole of it, as the server would receive
/// it, to `buf`. A test convenience over [`encode_command`].
#[cfg(test)]
pub(super) fn encode_command_to_buf(
    buf: &mut bytes::BytesMut,
    tag: &str,
    command: &Command,
    opts: &EncodeOptions,
) -> Result<(), crate::Error> {
    buf.extend_from_slice(&encode_command(tag, command, opts)?.to_vec());
    Ok(())
}
