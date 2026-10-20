use std::collections::{HashMap, HashSet};

use bifrost_types::FlagOp;

use crate::types::GmailLabel;

const LABEL_UNREAD: &str = "UNREAD";
const LABEL_STARRED: &str = "STARRED";
const LABEL_DRAFT: &str = "DRAFT";
const LABEL_SENT: &str = "SENT";
const LABEL_IMPORTANT: &str = "IMPORTANT";

/// Gmail's mutually exclusive display containers.
///
/// Gmail shows a message in whichever of these it carries, whatever
/// else is also attached: a spam message filed into a user label is
/// still rendered under Spam. So a relocation has to strip every one
/// of them the message is *not* moving into; adding the destination
/// alone is a no-op from the user's point of view.
pub(crate) const LABEL_INBOX: &str = "INBOX";
pub(crate) const LABEL_SPAM: &str = "SPAM";
pub(crate) const LABEL_TRASH: &str = "TRASH";
const EXCLUSIVE_CONTAINERS: [&str; 3] = [LABEL_INBOX, LABEL_SPAM, LABEL_TRASH];

/// Synthetic bifrost container id for Gmail archive.
///
/// Gmail models archive as the *absence* of every exclusive container
/// rather than as a label, so `containers_list` synthesises this id
/// purely to give the role table an `Archive` entry. It is not a real
/// Gmail label id and must never reach `addLabelIds` - Gmail rejects
/// the modify if it does.
pub(crate) const ARCHIVE_ID: &str = "archive";

pub(crate) fn is_archive_id(id: &str) -> bool {
    id.eq_ignore_ascii_case(ARCHIVE_ID)
}

/// The single Gmail relocation rule, shared by the bulk driver and the
/// single-object builders so a consumer cannot get different wire
/// semantics depending on which entry point it reached.
///
/// "Move into `destination`" lowers to: add `destination` (unless it is
/// the synthetic archive, which has no label to add), and remove every
/// exclusive display container that is not the destination itself.
///
/// - `INBOX` -> add INBOX, drop SPAM + TRASH (un-spam / un-trash).
/// - `archive` -> add nothing, drop INBOX + SPAM + TRASH.
/// - a user label -> add it, drop INBOX + SPAM + TRASH, so filing a
///   spammed or trashed message actually takes it out of Spam / Trash.
/// - `SPAM` / `TRASH` -> add it, drop the other two.
pub(crate) fn move_placement_patch(destination: &str) -> LabelPatch {
    let canonical_destination = EXCLUSIVE_CONTAINERS
        .iter()
        .find(|container| destination.eq_ignore_ascii_case(container))
        .copied()
        .unwrap_or(destination);
    let add_label_ids = if is_archive_id(canonical_destination) {
        Vec::new()
    } else {
        vec![canonical_destination.to_string()]
    };
    let remove_label_ids = EXCLUSIVE_CONTAINERS
        .iter()
        .filter(|container| !destination.eq_ignore_ascii_case(container))
        .map(|container| (*container).to_string())
        .collect();
    LabelPatch {
        add_label_ids,
        remove_label_ids,
        unsupported_flags: Vec::new(),
    }
}

const FLAG_SEEN: &str = "\\Seen";
const FLAG_FLAGGED: &str = "\\Flagged";
const FLAG_DRAFT: &str = "\\Draft";
const FLAG_IMPORTANT: &str = "$Important";
const USER_LABEL_FLAG_PREFIX: &str = "$gmail-label:";

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct LabelPatch {
    pub(crate) add_label_ids: Vec<String>,
    pub(crate) remove_label_ids: Vec<String>,
    pub(crate) unsupported_flags: Vec<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct CanonicalFlags {
    pub(crate) flags: Vec<String>,
    pub(crate) hash: u64,
}

pub(crate) type LabelNameIndex = HashMap<String, String>;

pub(crate) fn label_name_index(labels: &[GmailLabel]) -> LabelNameIndex {
    labels
        .iter()
        .map(|label| (label.id.clone(), label.name.clone()))
        .collect()
}

pub(crate) fn canonical_flags(label_ids: &[String], labels: &[GmailLabel]) -> CanonicalFlags {
    let names_by_id = label_name_index(labels);
    canonical_flags_indexed(label_ids, &names_by_id)
}

pub(crate) fn canonical_flags_indexed(
    label_ids: &[String],
    names_by_id: &LabelNameIndex,
) -> CanonicalFlags {
    let mut flags = Vec::new();

    if !label_ids
        .iter()
        .any(|label| label.eq_ignore_ascii_case(LABEL_UNREAD))
    {
        flags.push(FLAG_SEEN.to_string());
    }

    for label_id in label_ids {
        if [
            LABEL_UNREAD,
            LABEL_INBOX,
            LABEL_SENT,
            LABEL_TRASH,
            LABEL_SPAM,
            "CHAT",
        ]
        .iter()
        .any(|system| label_id.eq_ignore_ascii_case(system))
        {
            continue;
        }
        if label_id.eq_ignore_ascii_case(LABEL_STARRED) {
            flags.push(FLAG_FLAGGED.to_string());
        } else if label_id.eq_ignore_ascii_case(LABEL_DRAFT) {
            flags.push(FLAG_DRAFT.to_string());
        } else if label_id.eq_ignore_ascii_case(LABEL_IMPORTANT) {
            flags.push(FLAG_IMPORTANT.to_string());
        } else {
            let name = names_by_id
                .get(label_id.as_str())
                .map(String::as_str)
                .unwrap_or(label_id);
            flags.push(format!("$gmail-label:{label_id}:{name}"));
        }
    }

    flags.sort();
    flags.dedup();
    let hash = bifrost_types::canonical_flags_hash(&flags);
    CanonicalFlags { flags, hash }
}

pub(crate) fn flag_set_indexed(
    label_ids: &[String],
    names_by_id: &LabelNameIndex,
) -> HashSet<String> {
    canonical_flags_indexed(label_ids, names_by_id)
        .flags
        .into_iter()
        .collect()
}

pub(crate) fn flag_set(label_ids: &[String], labels: &[GmailLabel]) -> HashSet<String> {
    canonical_flags(label_ids, labels)
        .flags
        .into_iter()
        .collect()
}

pub(crate) fn translate_flag_op(op: &FlagOp, labels: &[GmailLabel]) -> LabelPatch {
    match op {
        FlagOp::Add(flags) => patch_from_sets(flags, &HashSet::new(), labels),
        FlagOp::Remove(flags) => patch_from_sets(&HashSet::new(), flags, labels),
        FlagOp::Patch { add, remove } => patch_from_sets(add, remove, labels),
        FlagOp::Set(flags) => patch_for_set(flags, labels),
        _ => LabelPatch {
            unsupported_flags: vec!["unknown future FlagOp variant".to_string()],
            ..LabelPatch::default()
        },
    }
}

fn patch_for_set(flags: &HashSet<String>, labels: &[GmailLabel]) -> LabelPatch {
    let mut add = HashSet::new();
    let mut remove = HashSet::new();

    if contains_flag(flags, FLAG_SEEN) {
        add.insert(FLAG_SEEN.to_string());
    } else {
        remove.insert(FLAG_SEEN.to_string());
    }

    for flag in [FLAG_FLAGGED, FLAG_IMPORTANT] {
        if contains_flag(flags, flag) {
            add.insert(flag.to_string());
        } else {
            remove.insert(flag.to_string());
        }
    }

    // `Set` means "the flag set is exactly this", so an omitted user label
    // has to be removed, not just left alone. We cannot know which labels
    // the message currently carries from here, so the patch names every
    // user label in the vocabulary - `batchModify` ignores removals for
    // labels a message does not have. Consequence to keep in mind:
    // `remove_label_ids` scales with the account's user label count, not
    // with the size of the incoming flag set. Gmail documents a cap of 100
    // label ids per update, and an account's label vocabulary is allowed to
    // be much larger than that, so the submitter splits the list across
    // several `batchModify` bodies rather than letting a label-heavy
    // account 400 on every exact-set - see `mutation::batch_modify_bodies`.
    //
    // Read-back-then-diff was weighed as the replacement and rejected on
    // correctness, not on effort. One `LabelPatch` is computed ONCE per
    // mutation and applied to up to 1000 ids in a single `batchModify`,
    // which is only sound because this patch is state-independent: it
    // asserts the same absolute set for every target regardless of what
    // any of them currently carries, so it is idempotent and immune to a
    // concurrent label change landing between the decision and the write.
    // A diffed patch is per-message by construction, so it would (a) need
    // a `messages.get` per target - 5 quota units each - to learn current
    // state, against a request body of a few KB, (b) shatter one
    // `batchModify` into one call per distinct diff, and (c) open a
    // lost-update window this shape does not have: a label added by
    // another client after the read is absent from the diff's
    // `removeLabelIds` and survives an exact-set that was supposed to
    // clear it. The engine's read-back guard does not close that window
    // either, since it verifies AFTER the write rather than supplying
    // pre-state. Trading a few KB of body for N reads, N writes and a
    // race is not a trade; the body size stays.
    //
    // The `user` filter is the point of this loop and is NOT the same
    // mistake as type-filtering `user_label_id_from_flag`. Gmail's
    // classifier owns the `CATEGORY_*` system labels; stripping them on
    // every `Set` would fight it. Those flags therefore round-trip as
    // no-ops here: resolvable (so they never poison `unsupported_flags`),
    // but never added or removed by an exact-set operation.
    for label in labels
        .iter()
        .filter(|label| label.label_type.as_deref() == Some("user"))
    {
        let flag = format!("{USER_LABEL_FLAG_PREFIX}{}:{}", label.id, label.name);
        if flags.iter().any(|candidate| {
            user_label_id_from_flag(candidate, labels).as_deref() == Some(label.id.as_str())
        }) {
            add.insert(flag);
        } else {
            remove.insert(flag);
        }
    }

    for flag in flags {
        // Unknown flags land in `add` so the reverse translation reports
        // them as unsupported. So do flags that ASSERT a read-only Gmail
        // projection (`\Draft`, a crafted `$gmail-label:SENT:...`): Gmail
        // refuses DRAFT and SENT in either label list, so an exact-set
        // demanding them cannot be honoured and must say so rather than
        // report a state it never reached.
        //
        // Their OMISSION from an exact set is deliberately not reported.
        // Draft-ness and sent-ness are structural in Gmail, not toggles;
        // an exact set that omits them is the ordinary case for every
        // ordinary message, and reporting each one as partially applied
        // would make `Applied` unreachable for `FlagOp::Set` forever
        // while saying nothing a consumer could act on. The residual gap
        // - a Set that omits `\Draft` against a message that really is a
        // draft - needs a read-back the translation layer does not have,
        // which makes it a `bifrost-sync` question rather than a Gmail
        // translation one. Accepted, not open.
        if !is_known_flag(flag, labels) || asserts_read_only_state(flag, labels) {
            add.insert(flag.clone());
        }
    }

    patch_from_sets(&add, &remove, labels)
}

fn patch_from_sets(
    add_flags: &HashSet<String>,
    remove_flags: &HashSet<String>,
    labels: &[GmailLabel],
) -> LabelPatch {
    let mut patch = LabelPatch::default();
    for flag in add_flags {
        match flag_to_add_label(flag, labels) {
            FlagTranslation::Add(label) => push_unique(&mut patch.add_label_ids, label),
            FlagTranslation::Remove(label) => push_unique(&mut patch.remove_label_ids, label),
            FlagTranslation::Unsupported(flag) => push_unique(&mut patch.unsupported_flags, flag),
        }
    }
    for flag in remove_flags {
        match flag_to_remove_label(flag, labels) {
            FlagTranslation::Add(label) => push_unique(&mut patch.add_label_ids, label),
            FlagTranslation::Remove(label) => push_unique(&mut patch.remove_label_ids, label),
            FlagTranslation::Unsupported(flag) => push_unique(&mut patch.unsupported_flags, flag),
        }
    }
    patch.add_label_ids.sort();
    patch.remove_label_ids.sort();
    patch.unsupported_flags.sort();
    patch
}

enum FlagTranslation {
    Add(String),
    Remove(String),
    Unsupported(String),
}

fn flag_to_add_label(flag: &str, labels: &[GmailLabel]) -> FlagTranslation {
    if eq_flag(flag, FLAG_SEEN) {
        FlagTranslation::Remove(LABEL_UNREAD.to_string())
    } else if eq_flag(flag, FLAG_FLAGGED) {
        FlagTranslation::Add(LABEL_STARRED.to_string())
    } else if eq_flag(flag, FLAG_DRAFT) {
        FlagTranslation::Unsupported(flag.to_string())
    } else if eq_flag(flag, FLAG_IMPORTANT) {
        FlagTranslation::Add(LABEL_IMPORTANT.to_string())
    } else if let Some(label_id) = user_label_id_from_flag(flag, labels) {
        if is_read_only_label(&label_id) {
            FlagTranslation::Unsupported(flag.to_string())
        } else {
            FlagTranslation::Add(label_id)
        }
    } else {
        FlagTranslation::Unsupported(flag.to_string())
    }
}

fn flag_to_remove_label(flag: &str, labels: &[GmailLabel]) -> FlagTranslation {
    if eq_flag(flag, FLAG_SEEN) {
        FlagTranslation::Add(LABEL_UNREAD.to_string())
    } else if eq_flag(flag, FLAG_FLAGGED) {
        FlagTranslation::Remove(LABEL_STARRED.to_string())
    } else if eq_flag(flag, FLAG_DRAFT) {
        FlagTranslation::Unsupported(flag.to_string())
    } else if eq_flag(flag, FLAG_IMPORTANT) {
        FlagTranslation::Remove(LABEL_IMPORTANT.to_string())
    } else if let Some(label_id) = user_label_id_from_flag(flag, labels) {
        if is_read_only_label(&label_id) {
            FlagTranslation::Unsupported(flag.to_string())
        } else {
            FlagTranslation::Remove(label_id)
        }
    } else {
        FlagTranslation::Unsupported(flag.to_string())
    }
}

/// Gmail's `DRAFT` and `SENT` are read-only projections of a message's
/// structure, not labels a client may attach or detach: `batchModify`
/// answers 400 for either id in `addLabelIds` or in `removeLabelIds`.
///
/// They therefore never reach the wire. That is only half the answer -
/// a flag we cannot send is a flag we did not apply, so the translation
/// routes them into `unsupported_flags` rather than dropping them, and
/// the mutation driver reports the incomplete result instead of a
/// success it never achieved.
fn is_read_only_label(label: &str) -> bool {
    label.eq_ignore_ascii_case(LABEL_DRAFT) || label.eq_ignore_ascii_case(LABEL_SENT)
}

/// True when `flag` asserts that a read-only Gmail projection is present.
fn asserts_read_only_state(flag: &str, labels: &[GmailLabel]) -> bool {
    eq_flag(flag, FLAG_DRAFT)
        || user_label_id_from_flag(flag, labels).is_some_and(|id| is_read_only_label(&id))
}

/// Resolve a `$gmail-label:<id>:<name>` flag back to a writable Gmail label.
///
/// The id is the stable key and the embedded display name is advisory, so
/// a label renamed on the server between the read that minted the flag and
/// the write that replays it still resolves. Splitting on the *first* `:`
/// after the prefix is deliberate: ids never contain a colon, names may.
///
/// Only user labels and Gmail's `CATEGORY_*` system labels resolve through
/// this generic flag spelling. `canonical_flags` emits it for both groups,
/// so filtering to user labels alone would break category round-trips. The
/// other system labels already have dedicated meanings (or are structural),
/// and accepting a caller-forged spelling such as `$gmail-label:TRASH:TRASH`
/// would bypass `move_placement_patch` or contradict `\\Seen` translation.
fn user_label_id_from_flag(flag: &str, labels: &[GmailLabel]) -> Option<String> {
    let (id, _advisory_name) = flag.strip_prefix(USER_LABEL_FLAG_PREFIX)?.split_once(':')?;
    labels
        .iter()
        .find(|label| {
            label.id == id
                && (label.label_type.as_deref() == Some("user") || is_category_label_id(&label.id))
        })
        .map(|label| label.id.clone())
}

fn is_category_label_id(id: &str) -> bool {
    id.get(.."CATEGORY_".len())
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("CATEGORY_"))
}

fn contains_flag(flags: &HashSet<String>, needle: &str) -> bool {
    flags.iter().any(|flag| eq_flag(flag, needle))
}

fn is_known_flag(flag: &str, labels: &[GmailLabel]) -> bool {
    eq_flag(flag, FLAG_SEEN)
        || eq_flag(flag, FLAG_FLAGGED)
        || eq_flag(flag, FLAG_DRAFT)
        || eq_flag(flag, FLAG_IMPORTANT)
        || user_label_id_from_flag(flag, labels).is_some()
}

fn eq_flag(left: &str, right: &str) -> bool {
    left.eq_ignore_ascii_case(right)
}

fn push_unique(items: &mut Vec<String>, item: String) {
    if !items.contains(&item) {
        items.push(item);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|item| (*item).to_string()).collect()
    }

    #[test]
    fn canonicalizes_gmail_labels_to_engine_flags() {
        let labels = vec![GmailLabel {
            id: "Label_1".to_string(),
            name: "Project".to_string(),
            label_type: Some("user".to_string()),
            color: None,
        }];
        let canonical = canonical_flags(
            &[
                "INBOX".to_string(),
                LABEL_STARRED.to_string(),
                LABEL_IMPORTANT.to_string(),
                "Label_1".to_string(),
            ],
            &labels,
        );
        assert_eq!(
            canonical.flags,
            vec![
                "$Important".to_string(),
                "$gmail-label:Label_1:Project".to_string(),
                "\\Flagged".to_string(),
                "\\Seen".to_string(),
            ]
        );
        let again = canonical_flags(
            &[
                "INBOX".to_string(),
                LABEL_STARRED.to_string(),
                LABEL_IMPORTANT.to_string(),
                "Label_1".to_string(),
            ],
            &labels,
        );
        assert_eq!(canonical.hash, again.hash);
    }

    #[test]
    fn unread_label_removes_seen_flag() {
        let canonical = canonical_flags(&[LABEL_UNREAD.to_string()], &[]);
        assert!(!canonical.flags.contains(&FLAG_SEEN.to_string()));
    }

    #[test]
    fn translates_seen_polarity_back_to_gmail_unread() {
        let patch = translate_flag_op(&FlagOp::Add(set(&[FLAG_SEEN, FLAG_FLAGGED])), &[]);
        assert_eq!(patch.add_label_ids, vec![LABEL_STARRED.to_string()]);
        assert_eq!(patch.remove_label_ids, vec![LABEL_UNREAD.to_string()]);
        assert!(patch.unsupported_flags.is_empty());
    }

    #[test]
    fn remove_seen_adds_unread() {
        let patch = translate_flag_op(&FlagOp::Remove(set(&[FLAG_SEEN])), &[]);
        assert_eq!(patch.add_label_ids, vec![LABEL_UNREAD.to_string()]);
        assert!(patch.remove_label_ids.is_empty());
    }

    #[test]
    fn set_seen_removes_unread() {
        let patch = translate_flag_op(&FlagOp::Set(set(&[FLAG_SEEN])), &[]);
        assert!(patch.add_label_ids.is_empty());
        assert_eq!(
            patch.remove_label_ids,
            vec![
                LABEL_IMPORTANT.to_string(),
                LABEL_STARRED.to_string(),
                LABEL_UNREAD.to_string()
            ]
        );
    }

    #[test]
    fn move_to_inbox_drops_the_other_exclusive_containers() {
        let patch = move_placement_patch(LABEL_INBOX);
        assert_eq!(patch.add_label_ids, vec![LABEL_INBOX.to_string()]);
        assert_eq!(
            patch.remove_label_ids,
            vec![LABEL_SPAM.to_string(), LABEL_TRASH.to_string()]
        );
    }

    #[test]
    fn move_to_user_label_drops_spam_and_trash_too() {
        let patch = move_placement_patch("Label_42");
        assert_eq!(patch.add_label_ids, vec!["Label_42".to_string()]);
        assert_eq!(
            patch.remove_label_ids,
            vec![
                LABEL_INBOX.to_string(),
                LABEL_SPAM.to_string(),
                LABEL_TRASH.to_string()
            ],
            "filing a spammed or trashed message must take it out of Spam / Trash"
        );
    }

    #[test]
    fn move_to_archive_adds_no_label() {
        let patch = move_placement_patch(ARCHIVE_ID);
        assert!(
            patch.add_label_ids.is_empty(),
            "`archive` is synthetic and is not a Gmail label id"
        );
        assert_eq!(
            patch.remove_label_ids,
            vec![
                LABEL_INBOX.to_string(),
                LABEL_SPAM.to_string(),
                LABEL_TRASH.to_string()
            ]
        );
    }

    #[test]
    fn move_to_spam_keeps_spam_and_drops_the_rest() {
        let patch = move_placement_patch(LABEL_SPAM);
        assert_eq!(patch.add_label_ids, vec![LABEL_SPAM.to_string()]);
        assert_eq!(
            patch.remove_label_ids,
            vec![LABEL_INBOX.to_string(), LABEL_TRASH.to_string()]
        );
    }

    #[test]
    fn exclusive_container_match_is_case_insensitive() {
        let patch = move_placement_patch("inbox");
        assert_eq!(patch.add_label_ids, vec![LABEL_INBOX.to_string()]);
        assert_eq!(
            patch.remove_label_ids,
            vec![LABEL_SPAM.to_string(), LABEL_TRASH.to_string()],
            "a lowercased INBOX must not ask Gmail to remove the container it is moving into"
        );
    }

    #[test]
    fn set_without_seen_adds_unread() {
        let patch = translate_flag_op(&FlagOp::Set(set(&[FLAG_FLAGGED])), &[]);
        assert_eq!(
            patch.add_label_ids,
            vec![LABEL_STARRED.to_string(), LABEL_UNREAD.to_string()]
        );
        assert_eq!(patch.remove_label_ids, vec![LABEL_IMPORTANT.to_string()]);
    }

    fn work_label() -> Vec<GmailLabel> {
        vec![GmailLabel {
            id: "Label_1".to_string(),
            name: "Work".to_string(),
            label_type: Some("user".to_string()),
            color: None,
        }]
    }

    /// A user-label flag whose spelling matches `<id>:<name>` in the
    /// known vocabulary translates to the bare Gmail label id.
    #[test]
    fn user_label_flags_translate_to_their_gmail_label_id() {
        let patch = translate_flag_op(
            &FlagOp::Add(set(&["$gmail-label:Label_1:Work"])),
            &work_label(),
        );
        assert_eq!(patch.add_label_ids, vec!["Label_1".to_string()]);
        assert!(patch.unsupported_flags.is_empty());

        let patch = translate_flag_op(
            &FlagOp::Remove(set(&["$gmail-label:Label_1:Work"])),
            &work_label(),
        );
        assert_eq!(patch.remove_label_ids, vec!["Label_1".to_string()]);
    }

    #[test]
    fn a_user_label_flag_with_a_stale_name_resolves_by_id() {
        let patch = translate_flag_op(
            &FlagOp::Add(set(&["$gmail-label:Label_1:OldName"])),
            &work_label(),
        );
        assert_eq!(patch.add_label_ids, vec!["Label_1".to_string()]);
        assert!(patch.unsupported_flags.is_empty());
    }

    /// Same shape, reached the other way: an empty label vocabulary
    /// (the state a freshly opened account's scope cache is in) cannot
    /// resolve any user label at all.
    #[test]
    fn an_empty_label_vocabulary_makes_every_user_label_unsupported() {
        let patch = translate_flag_op(&FlagOp::Add(set(&["$gmail-label:Label_1:Work"])), &[]);
        assert!(patch.add_label_ids.is_empty());
        assert_eq!(patch.unsupported_flags.len(), 1);
    }

    #[test]
    fn set_removes_user_labels_the_new_set_omits() {
        let patch = translate_flag_op(&FlagOp::Set(set(&[FLAG_SEEN])), &work_label());
        assert!(
            patch.remove_label_ids.contains(&"Label_1".to_string()),
            "an exact-set operation removes omitted known user labels",
        );
        assert_eq!(
            patch.remove_label_ids,
            vec![
                LABEL_IMPORTANT.to_string(),
                "Label_1".to_string(),
                LABEL_STARRED.to_string(),
                LABEL_UNREAD.to_string()
            ]
        );
    }

    /// Regression guard. `canonical_flags` has no canonical spelling for
    /// Gmail's system category labels, so it renders them through the
    /// `$gmail-label:` fallback exactly like a user label. If
    /// `user_label_id_from_flag` ever grows a `label_type == "user"`
    /// filter again, this round trip breaks and every affected batch
    /// fails as malformed instead of applying the resolvable label.
    #[test]
    fn a_system_category_label_flag_round_trips_through_translation() {
        let labels = vec![GmailLabel {
            id: "CATEGORY_PROMOTIONS".to_string(),
            name: "CATEGORY_PROMOTIONS".to_string(),
            label_type: Some("system".to_string()),
            color: None,
        }];
        let canonical = canonical_flags(&["CATEGORY_PROMOTIONS".to_string()], &labels);
        assert!(
            canonical
                .flags
                .contains(&"$gmail-label:CATEGORY_PROMOTIONS:CATEGORY_PROMOTIONS".to_string())
        );

        let patch = translate_flag_op(&FlagOp::Add(canonical.flags.into_iter().collect()), &labels);
        assert_eq!(patch.add_label_ids, vec!["CATEGORY_PROMOTIONS".to_string()]);
        assert!(
            patch.unsupported_flags.is_empty(),
            "a flag this crate itself minted must translate back"
        );
    }

    #[test]
    fn forged_container_and_unread_label_flags_never_reach_batch_modify() {
        let labels = ["INBOX", "TRASH", "UNREAD"]
            .into_iter()
            .map(|id| GmailLabel {
                id: id.to_string(),
                name: id.to_string(),
                label_type: Some("system".to_string()),
                color: None,
            })
            .collect::<Vec<_>>();
        let patch = translate_flag_op(
            &FlagOp::Add(set(&[
                "$gmail-label:INBOX:INBOX",
                "$gmail-label:TRASH:TRASH",
                "$gmail-label:UNREAD:UNREAD",
            ])),
            &labels,
        );

        assert!(patch.add_label_ids.is_empty());
        assert!(patch.remove_label_ids.is_empty());
        assert_eq!(
            patch.unsupported_flags,
            vec![
                "$gmail-label:INBOX:INBOX".to_string(),
                "$gmail-label:TRASH:TRASH".to_string(),
                "$gmail-label:UNREAD:UNREAD".to_string(),
            ]
        );

        let contradictory = translate_flag_op(
            &FlagOp::Add(set(&["\\Seen", "$gmail-label:UNREAD:UNREAD"])),
            &labels,
        );
        assert_eq!(contradictory.remove_label_ids, vec!["UNREAD".to_string()]);
        assert!(contradictory.add_label_ids.is_empty());
        assert_eq!(
            contradictory.unsupported_flags,
            vec!["$gmail-label:UNREAD:UNREAD".to_string()]
        );
    }

    /// The other half of the category-label contract: resolvable, but
    /// left alone by an exact set in both directions, because Gmail's
    /// classifier owns those labels.
    #[test]
    fn set_neither_adds_nor_removes_system_category_labels() {
        let labels = vec![GmailLabel {
            id: "CATEGORY_SOCIAL".to_string(),
            name: "CATEGORY_SOCIAL".to_string(),
            label_type: Some("system".to_string()),
            color: None,
        }];
        let omitted = translate_flag_op(&FlagOp::Set(set(&[FLAG_SEEN])), &labels);
        assert!(
            !omitted
                .remove_label_ids
                .contains(&"CATEGORY_SOCIAL".to_string())
        );
        assert!(omitted.unsupported_flags.is_empty());

        let named = translate_flag_op(
            &FlagOp::Set(set(&[
                FLAG_SEEN,
                "$gmail-label:CATEGORY_SOCIAL:CATEGORY_SOCIAL",
            ])),
            &labels,
        );
        assert!(!named.add_label_ids.contains(&"CATEGORY_SOCIAL".to_string()));
        assert!(named.unsupported_flags.is_empty());
    }

    /// Documents the cost of exact-set semantics: the removal list is
    /// sized by the account's user label vocabulary, not by the incoming
    /// flag set, because the patch is built without knowledge of what the
    /// target messages currently carry.
    #[test]
    fn set_removals_scale_with_the_user_label_vocabulary() {
        let labels = (0..50)
            .map(|n| GmailLabel {
                id: format!("Label_{n}"),
                name: format!("Name {n}"),
                label_type: Some("user".to_string()),
                color: None,
            })
            .collect::<Vec<_>>();
        let patch = translate_flag_op(&FlagOp::Set(set(&[FLAG_SEEN])), &labels);
        assert_eq!(
            patch.remove_label_ids.len(),
            50 + 3,
            "every known user label, plus STARRED / IMPORTANT for the \
             omitted canonical flags and UNREAD for the asserted \\Seen"
        );
    }

    #[test]
    fn set_keeps_a_user_label_selected_with_its_stale_name() {
        let patch = translate_flag_op(
            &FlagOp::Set(set(&[FLAG_SEEN, "$gmail-label:Label_1:OldName"])),
            &work_label(),
        );
        assert!(patch.add_label_ids.contains(&"Label_1".to_string()));
        assert!(!patch.remove_label_ids.contains(&"Label_1".to_string()));
        assert!(patch.unsupported_flags.is_empty());
    }

    /// An unknown flag inside a `Set` is added verbatim and then fails
    /// to translate, so the whole patch is poisoned into the
    /// unsupported lane rather than applying the parts it understood.
    #[test]
    fn an_unknown_flag_inside_a_set_poisons_the_whole_patch() {
        let patch = translate_flag_op(&FlagOp::Set(set(&[FLAG_SEEN, "$Junk"])), &[]);
        assert_eq!(patch.unsupported_flags, vec!["$Junk".to_string()]);
        assert!(
            !patch.remove_label_ids.is_empty(),
            "the canonical half still translates; the driver fails on unsupported_flags",
        );
    }

    /// Flag comparison is ASCII-case-insensitive, matching IMAP
    /// keyword semantics, so `\seen` and `\Seen` are the same flag.
    #[test]
    fn canonical_flag_matching_is_case_insensitive() {
        let patch = translate_flag_op(&FlagOp::Add(set(&["\\sEeN", "$important"])), &[]);
        assert_eq!(patch.remove_label_ids, vec![LABEL_UNREAD.to_string()]);
        assert_eq!(patch.add_label_ids, vec![LABEL_IMPORTANT.to_string()]);
        assert!(patch.unsupported_flags.is_empty());
    }

    /// Adding and removing the same flag in one `Patch` is a caller
    /// contradiction; the label lands in both lists and Gmail resolves
    /// it (removal wins). Pinned so the behaviour is a decision rather
    /// than an accident.
    #[test]
    fn a_contradictory_patch_lands_the_label_in_both_lists() {
        let patch = translate_flag_op(
            &FlagOp::Patch {
                add: set(&[FLAG_FLAGGED]),
                remove: set(&[FLAG_FLAGGED]),
            },
            &[],
        );
        assert_eq!(patch.add_label_ids, vec![LABEL_STARRED.to_string()]);
        assert_eq!(patch.remove_label_ids, vec![LABEL_STARRED.to_string()]);
    }

    #[test]
    fn empty_flag_ops_produce_an_empty_patch() {
        let patch = translate_flag_op(&FlagOp::Add(HashSet::new()), &[]);
        assert_eq!(patch, LabelPatch::default());
        let patch = translate_flag_op(&FlagOp::Remove(HashSet::new()), &[]);
        assert_eq!(patch, LabelPatch::default());
    }

    /// The canonical projection is order-independent: Gmail returns
    /// `labelIds` in no guaranteed order, so the hash must not depend
    /// on it.
    #[test]
    fn canonical_flags_are_order_independent_and_deduplicated() {
        let forward = canonical_flags(
            &[
                LABEL_STARRED.to_string(),
                LABEL_IMPORTANT.to_string(),
                "Label_1".to_string(),
            ],
            &work_label(),
        );
        let reversed = canonical_flags(
            &[
                "Label_1".to_string(),
                LABEL_IMPORTANT.to_string(),
                LABEL_STARRED.to_string(),
            ],
            &work_label(),
        );
        assert_eq!(forward.flags, reversed.flags);
        assert_eq!(forward.hash, reversed.hash);

        let duplicated =
            canonical_flags(&[LABEL_STARRED.to_string(), LABEL_STARRED.to_string()], &[]);
        assert_eq!(
            duplicated.flags,
            vec![FLAG_FLAGGED.to_string(), FLAG_SEEN.to_string()]
        );
    }

    /// Folder-like Gmail labels are containers, not flags, and must not
    /// leak into the canonical flag set.
    #[test]
    fn folder_shaped_labels_drop_out_of_the_flag_projection() {
        let canonical = canonical_flags(
            &[
                LABEL_INBOX.to_string(),
                "SENT".to_string(),
                LABEL_TRASH.to_string(),
                LABEL_SPAM.to_string(),
                "CHAT".to_string(),
            ],
            &[],
        );
        assert_eq!(
            canonical.flags,
            vec![FLAG_SEEN.to_string()],
            "only the derived \\Seen survives a folder-only label set"
        );
    }

    /// A label id missing from a freshly fetched vocabulary uses its
    /// stable id as the display-name fallback. A later vocabulary
    /// refresh can update the display-bearing flag and its hash.
    #[test]
    fn an_unknown_label_id_renders_its_id_in_the_name_slot() {
        let canonical = canonical_flags(&["Label_9".to_string()], &[]);
        assert!(
            canonical
                .flags
                .contains(&"$gmail-label:Label_9:Label_9".to_string())
        );
    }

    #[test]
    fn system_label_projection_is_case_insensitive() {
        let upper = canonical_flags(&[LABEL_UNREAD.to_string()], &[]);
        assert!(!upper.flags.contains(&FLAG_SEEN.to_string()));

        let lower = canonical_flags(&["unread".to_string()], &[]);
        assert!(
            !lower.flags.contains(&FLAG_SEEN.to_string()),
            "a lowercased UNREAD still projects as unread",
        );
        assert!(
            lower.flags.is_empty(),
            "a system label must not leak into the user-label namespace"
        );
    }

    /// One label spelled `ab` and two labels spelled `a` and `b` must
    /// not collide. The `0xff` terminator the hash mixes after every
    /// flag is what separates them.
    #[test]
    fn one_long_label_does_not_hash_like_two_short_ones() {
        let joined = canonical_flags(&["Label_ab".to_string()], &[]);
        let split = canonical_flags(&["Label_a".to_string(), "Label_b".to_string()], &[]);
        assert_ne!(joined.hash, split.hash);
    }
}
