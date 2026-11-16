use crate::types::Command;

use super::commands::{
    encode_append, encode_authenticate, encode_create_special_use, encode_fetch,
    encode_getmetadata, encode_id, encode_list_extended, encode_list_status, encode_login,
    encode_mailbox_argument, encode_mailbox_cmd, encode_mailbox_name, encode_notify_set,
    encode_search, encode_select_or_examine, encode_set_acl, encode_set_quota, encode_setmetadata,
    encode_simple, encode_status, encode_store, encode_thread_or_sort_cmd, encode_two_arg,
    encode_two_quoted_args, encode_uid_expunge, encode_uid_fetch,
};
use super::{CommandWriter, EncodeOptions, WireCommand, encode_enable, validate_atom};

/// Encode command syntax using the negotiated wire representation.
/// Session and extension admission belongs to `connection::admission`.
/// Syntax validation remains here so malformed data can never reach the wire.
#[allow(clippy::too_many_lines)]
pub(crate) fn encode_command(
    tag: &str,
    command: &Command,
    opts: &EncodeOptions,
) -> Result<WireCommand, crate::Error> {
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
            encode_simple(&mut w, tag, "STARTTLS");
        }
        // RFC 3501 Section 6.1.3 / RFC 9051 Section 6.1.3
        Command::Logout => {
            encode_simple(&mut w, tag, "LOGOUT");
        }
        // RFC 5161 Section 3
        Command::Enable { capabilities } => {
            encode_enable(&mut w, tag, capabilities)?;
        }
        Command::List { reference, pattern } => {
            let wire_ref = encode_mailbox_argument(reference, utf8)?;
            let wire_pat = encode_mailbox_argument(pattern, utf8)?;
            w.raw(tag.as_bytes());
            w.raw(b" LIST ");
            w.string(&wire_ref, utf8);
            w.raw(b" ");
            w.string(&wire_pat, utf8);
            w.raw(b"\r\n");
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
            let wire_ref = encode_mailbox_argument(reference, utf8)?;
            let wire_pat = encode_mailbox_argument(pattern, utf8)?;
            w.raw(tag.as_bytes());
            w.raw(b" LSUB ");
            w.string(&wire_ref, utf8);
            w.raw(b" ");
            w.string(&wire_pat, utf8);
            w.raw(b"\r\n");
        }
        // Admission has established MULTIAPPEND and BINARY eligibility.
        // Encoding retains shape, flag and date validation and chooses body
        // framing from the live negotiated options.
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
            encode_simple(&mut w, tag, "UNSELECT");
        }
        Command::Status { mailbox, items } => {
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_status(&mut w, tag, &wire, items, utf8)?;
        }
        Command::Search {
            addressing,
            criteria,
        } => {
            encode_search(
                &mut w,
                tag,
                addressing.command("SEARCH", "UID SEARCH"),
                criteria,
                None,
                utf8,
            )?;
        }
        Command::SearchReturn {
            addressing,
            criteria,
            return_opts,
        } => {
            encode_search(
                &mut w,
                tag,
                addressing.command("SEARCH", "UID SEARCH"),
                criteria,
                Some(return_opts),
                utf8,
            )?;
        }
        Command::SearchSave {
            addressing,
            criteria,
        } => {
            encode_search(
                &mut w,
                tag,
                addressing.command("SEARCH RETURN (SAVE)", "UID SEARCH RETURN (SAVE)"),
                criteria,
                None,
                utf8,
            )?;
        }
        Command::Fetch {
            sequence_set,
            items,
            changed_since,
        } => {
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_fetch(&mut w, tag, sequence_set.as_str(), items, *changed_since)?;
        }
        Command::Store {
            sequence_set,
            operation,
            flags,
            unchanged_since,
        } => {
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
        Command::UidFetch {
            sequence_set,
            items,
            changed_since,
            vanished,
        } => {
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
            // SequenceSet is pre-validated at construction time (RFC 3501 Section 9).
            encode_uid_expunge(&mut w, tag, sequence_set.as_str())?;
        }
        // RFC 2342 Section 5 / RFC 9051 Section 6.3.10
        Command::Namespace => {
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
            encode_id(&mut w, tag, params, utf8)?;
        }
        Command::GetMetadata {
            mailbox,
            entries,
            max_size,
            depth,
        } => {
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
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_setmetadata(&mut w, tag, &wire, entries, utf8)?;
        }
        Command::Thread {
            addressing,
            algorithm,
            charset,
            criteria,
        } => {
            encode_thread_or_sort_cmd(
                &mut w,
                tag,
                addressing.command("THREAD", "UID THREAD"),
                algorithm,
                charset,
                criteria,
                false,
            )?;
        }
        Command::Sort {
            addressing,
            sort_criteria,
            charset,
            criteria,
        } => {
            encode_thread_or_sort_cmd(
                &mut w,
                tag,
                addressing.command("SORT", "UID SORT"),
                sort_criteria,
                charset,
                criteria,
                true,
            )?;
        }
        Command::Compress => {
            w.raw(tag.as_bytes());
            w.raw(b" COMPRESS DEFLATE\r\n");
        }

        // --- QUOTA (RFC 2087) ---
        Command::GetQuota { root } => {
            encode_mailbox_cmd(&mut w, tag, "GETQUOTA", root, utf8);
        }
        Command::GetQuotaRoot { mailbox } => {
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_mailbox_cmd(&mut w, tag, "GETQUOTAROOT", &wire, utf8);
        }
        Command::SetQuota { root, resources } => {
            encode_set_quota(&mut w, tag, root, resources, utf8)?;
        }

        // --- ACL (RFC 4314) ---
        Command::SetAcl {
            mailbox,
            identifier,
            rights,
        } => {
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_set_acl(&mut w, tag, &wire, identifier, rights, utf8);
        }
        Command::DeleteAcl {
            mailbox,
            identifier,
        } => {
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_two_quoted_args(&mut w, tag, "DELETEACL", &wire, identifier, utf8);
        }
        Command::GetAcl { mailbox } => {
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_mailbox_cmd(&mut w, tag, "GETACL", &wire, utf8);
        }
        Command::ListRights {
            mailbox,
            identifier,
        } => {
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_two_quoted_args(&mut w, tag, "LISTRIGHTS", &wire, identifier, utf8);
        }
        Command::MyRights { mailbox } => {
            let wire = encode_mailbox_name(mailbox, utf8)?;
            encode_mailbox_cmd(&mut w, tag, "MYRIGHTS", &wire, utf8);
        }

        // --- NOTIFY (RFC 5465) ---
        Command::NotifySet(params) => {
            encode_notify_set(&mut w, tag, params, utf8)?;
        }
        Command::NotifyNone => {
            encode_simple(&mut w, tag, "NOTIFY NONE");
        }
    }
    w.finish()
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
