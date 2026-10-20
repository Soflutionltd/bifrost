use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Instant;

use crate::types::{MailboxAttribute, MailboxInfo, MailboxName, UidRange};

use super::FolderCursor;

/// Compact sorted UID set used in Basic and CONDSTORE cursor payloads.
// protocol-specific: IMAP cursors need a compact UID baseline, not a wire sequence set.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct CompactUidSet {
    ranges: Vec<UidRange>,
}

impl CompactUidSet {
    pub(crate) fn from_uids(uids: impl IntoIterator<Item = u32>) -> Self {
        let mut values: Vec<u32> = uids.into_iter().filter(|uid| *uid != 0).collect();
        values.sort_unstable();
        values.dedup();

        let mut ranges = Vec::new();
        let mut iter = values.into_iter();
        let Some(mut start) = iter.next() else {
            return Self { ranges };
        };
        let mut end = start;
        for uid in iter {
            if uid == end.saturating_add(1) {
                end = uid;
            } else {
                ranges.push(normalized_range(start, end));
                start = uid;
                end = uid;
            }
        }
        ranges.push(normalized_range(start, end));
        Self { ranges }
    }

    pub(crate) fn from_ranges(ranges: Vec<UidRange>) -> Self {
        let mut set = Self::default();
        for range in ranges {
            let start = range.start;
            let end = range.end.unwrap_or(start);
            if start != 0 && end >= start {
                set.insert_range(start, end);
            }
        }
        set
    }

    pub(crate) fn ranges(&self) -> &[UidRange] {
        &self.ranges
    }

    pub(crate) fn to_uids(&self) -> Vec<u32> {
        self.ranges.iter().copied().flat_map(expand_range).collect()
    }

    /// Number of individual UIDs represented by the compact ranges.
    pub(crate) fn uid_count(&self) -> usize {
        self.ranges
            .iter()
            .map(|range| {
                range
                    .end
                    .map(|end| end.saturating_sub(range.start).saturating_add(1))
                    .unwrap_or(1)
            })
            .map(|count| usize::try_from(count).unwrap_or(usize::MAX))
            .sum()
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.ranges.is_empty()
    }

    /// Membership test over the compact ranges. `from_uids` leaves them
    /// sorted and disjoint, so this is a binary search rather than an
    /// expansion of the whole baseline.
    pub(crate) fn contains(&self, uid: u32) -> bool {
        self.ranges
            .binary_search_by(|range| {
                let end = range.end.unwrap_or(range.start);
                if end < uid {
                    std::cmp::Ordering::Less
                } else if range.start > uid {
                    std::cmp::Ordering::Greater
                } else {
                    std::cmp::Ordering::Equal
                }
            })
            .is_ok()
    }

    /// Insert one UID while retaining sorted, disjoint, maximally merged ranges.
    /// Returns whether the UID was absent before the insertion.
    pub(crate) fn insert(&mut self, uid: u32) -> bool {
        if uid == 0 || self.contains(uid) {
            return false;
        }
        self.insert_range(uid, uid);
        true
    }

    /// Remove one UID while retaining sorted, disjoint ranges.
    /// Returns whether the UID was present before the removal.
    pub(crate) fn remove(&mut self, uid: u32) -> bool {
        let Ok(index) = self.ranges.binary_search_by(|range| {
            let end = range.end.unwrap_or(range.start);
            if end < uid {
                std::cmp::Ordering::Less
            } else if range.start > uid {
                std::cmp::Ordering::Greater
            } else {
                std::cmp::Ordering::Equal
            }
        }) else {
            return false;
        };
        let range = self.ranges[index];
        let end = range.end.unwrap_or(range.start);
        match (uid == range.start, uid == end) {
            (true, true) => {
                self.ranges.remove(index);
            }
            (true, false) => {
                self.ranges[index] = normalized_range(uid + 1, end);
            }
            (false, true) => {
                self.ranges[index] = normalized_range(range.start, uid - 1);
            }
            (false, false) => {
                self.ranges[index] = normalized_range(range.start, uid - 1);
                self.ranges
                    .insert(index + 1, normalized_range(uid + 1, end));
            }
        }
        true
    }

    fn insert_range(&mut self, mut start: u32, mut end: u32) {
        let index = self
            .ranges
            .partition_point(|range| range.end.unwrap_or(range.start).saturating_add(1) < start);
        while index < self.ranges.len() && self.ranges[index].start <= end.saturating_add(1) {
            let range = self.ranges.remove(index);
            start = start.min(range.start);
            end = end.max(range.end.unwrap_or(range.start));
        }
        self.ranges.insert(index, normalized_range(start, end));
    }

    /// Ascending UIDs, produced lazily from the ranges. Callers that only
    /// walk the set in order must use this rather than `to_uids`, which
    /// materialises one `u32` per UID.
    fn iter(&self) -> impl Iterator<Item = u32> + '_ {
        self.ranges
            .iter()
            .flat_map(|range| range.start..=range.end.unwrap_or(range.start))
    }

    /// Linear subtraction of two sorted range lists. Equal spans are skipped
    /// as ranges, so work is proportional to the number of ranges plus the
    /// UIDs that actually changed, not the total mailbox size.
    pub(crate) fn diff(&self, newer: &Self) -> UidSetDiff {
        UidSetDiff {
            added: subtract_ranges(&newer.ranges, &self.ranges),
            removed: subtract_ranges(&self.ranges, &newer.ranges),
        }
    }
}

fn subtract_ranges(source: &[UidRange], cover: &[UidRange]) -> Vec<u32> {
    let mut out = Vec::new();
    let mut cover_index = 0;
    for range in source {
        let end = u64::from(range.end.unwrap_or(range.start));
        let mut cursor = u64::from(range.start);
        while cover_index < cover.len()
            && u64::from(cover[cover_index].end.unwrap_or(cover[cover_index].start)) < cursor
        {
            cover_index += 1;
        }
        let mut index = cover_index;
        while index < cover.len() && u64::from(cover[index].start) <= end {
            let covered = cover[index];
            let covered_start = u64::from(covered.start);
            let covered_end = u64::from(covered.end.unwrap_or(covered.start));
            if cursor < covered_start {
                out.extend((cursor..covered_start.min(end + 1)).map(uid_from_wide));
            }
            cursor = cursor.max(covered_end + 1);
            if cursor > end {
                break;
            }
            index += 1;
        }
        if cursor <= end {
            out.extend((cursor..=end).map(uid_from_wide));
        }
        cover_index = index;
    }
    out
}

#[allow(clippy::cast_possible_truncation)]
fn uid_from_wide(uid: u64) -> u32 {
    debug_assert!(uid <= u64::from(u32::MAX));
    uid as u32
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct UidSetDiff {
    pub(crate) added: Vec<u32>,
    pub(crate) removed: Vec<u32>,
}

/// The one canonical spelling for an inclusive run of UIDs.
///
/// `UidRange::range(n, n)` and `UidRange::single(n)` are distinct values that
/// both denote a single UID, and `CompactUidSet` derives `PartialEq` over its
/// range vector. Every construction and mutation path must therefore agree on
/// which one it emits, or two sets holding identical UIDs compare unequal and
/// encode to different cursor payloads.
fn normalized_range(start: u32, end: u32) -> UidRange {
    if start == end {
        UidRange::single(start)
    } else {
        UidRange::range(start, end)
    }
}

pub(crate) fn expand_range(range: UidRange) -> Vec<u32> {
    match range.end {
        Some(end) => (range.start..=end).collect(),
        None => vec![range.start],
    }
}

pub(crate) struct FolderEntry {
    pub(crate) name: MailboxName,
    /// The LIST-derived facts that a mid-session LIST/IDLE announcement can
    /// change for an unchanged mailbox epoch. Held behind a lock rather than
    /// as plain fields so a refresh mutates the entry every holder already
    /// shares. Swapping in a replacement `Arc<FolderEntry>` instead would
    /// lose any cursor or MODSEQ write a task performed through an `Arc` it
    /// had cloned before the swap.
    listing: RwLock<FolderListing>,
    /// `Some(owner)` for a shared/other-user folder discovered under a
    /// non-personal namespace; `None` for the account's own personal
    /// folders. Drives `MembershipScope::Mailbox` tagging and
    /// `ErrorScope::Mailbox` scoping on revocation.
    pub(crate) shared_owner: Option<bifrost_types::MailboxId>,
    /// The MYRIGHTS rights set observed for this folder at discovery, when
    /// the server advertises ACL and answered. `None` means "not
    /// reported" (no ACL capability, MYRIGHTS failed, or a personal folder
    /// we never probed) - distinct from an explicit empty rights set.
    /// Projected onto `Container::rights` by `containers_list`; the
    /// authoritative per-operation gate is still the live `NO [ACL]`.
    pub(crate) rights: Option<crate::types::MailboxRights>,
    cursor: RwLock<Option<FolderCursor>>,
    modseq_by_uid: RwLock<ModSeqCache>,
    last_seen: Mutex<Option<Instant>>,
}

/// The mutable, LIST-sourced projection of one folder.
#[derive(Debug, Clone)]
pub(crate) struct FolderListing {
    pub(crate) selectable: bool,
    pub(crate) delimiter: Option<char>,
    pub(crate) attributes: Vec<MailboxAttribute>,
}

/// Per-folder ceiling on the opportunistic MODSEQ cache. Inventory walks
/// every UID in a mailbox, so without a bound a 500k-message folder holds a
/// permanent multi-MB map per folder for a cache that only ever saves a
/// round trip on `STORE UNCHANGEDSINCE`.
const MODSEQ_CACHE_CAPACITY: usize = 50_000;
/// What a prune leaves behind, so the eviction sort is amortised over many
/// inserts rather than running on every one past the ceiling.
const MODSEQ_CACHE_TARGET: usize = 40_000;

#[derive(Debug, Default, Clone)]
struct ModSeqCache {
    uidvalidity: Option<u32>,
    by_uid: HashMap<u32, u64>,
}

impl ModSeqCache {
    /// Evict down to `MODSEQ_CACHE_TARGET`, keeping the highest UIDs.
    ///
    /// UID order is arrival order in IMAP, and mutations overwhelmingly
    /// target recent mail, so "highest UID" is the cheap stand-in for
    /// "most likely to be asked for" - no access-order bookkeeping on a
    /// cache whose whole point is being free to maintain. A miss costs an
    /// unprotected STORE, which is the documented cold-cache behaviour.
    fn prune(&mut self) {
        if self.by_uid.len() <= MODSEQ_CACHE_CAPACITY {
            return;
        }
        let mut uids: Vec<u32> = self.by_uid.keys().copied().collect();
        let evict = uids.len() - MODSEQ_CACHE_TARGET;
        uids.select_nth_unstable(evict);
        for uid in &uids[..evict] {
            self.by_uid.remove(uid);
        }
    }
}

fn listing_from_info(
    info: &MailboxInfo,
    rights: Option<&crate::types::MailboxRights>,
) -> FolderListing {
    let listed_selectable = !info.attributes.iter().any(|attr| {
        matches!(
            attr,
            MailboxAttribute::NoSelect | MailboxAttribute::NonExistent
        )
    });
    FolderListing {
        selectable: shared_folder_is_selectable(listed_selectable, rights),
        delimiter: info.delimiter,
        attributes: info.attributes.clone(),
    }
}

impl FolderEntry {
    pub(crate) fn from_mailbox(info: MailboxInfo) -> Self {
        Self::from_mailbox_with_owner(info, None)
    }

    pub(crate) fn from_mailbox_with_owner(
        info: MailboxInfo,
        shared_owner: Option<bifrost_types::MailboxId>,
    ) -> Self {
        Self::from_mailbox_with_owner_rights(info, shared_owner, None)
    }

    pub(crate) fn from_mailbox_with_owner_rights(
        info: MailboxInfo,
        shared_owner: Option<bifrost_types::MailboxId>,
        rights: Option<crate::types::MailboxRights>,
    ) -> Self {
        let listing = listing_from_info(&info, rights.as_ref());
        Self {
            name: info.name,
            listing: RwLock::new(listing),
            shared_owner,
            rights,
            cursor: RwLock::new(None),
            modseq_by_uid: RwLock::new(ModSeqCache::default()),
            last_seen: Mutex::new(None),
        }
    }

    pub(crate) fn selectable(&self) -> bool {
        self.listing
            .read()
            .expect("folder listing lock poisoned")
            .selectable
    }

    pub(crate) fn delimiter(&self) -> Option<char> {
        self.listing
            .read()
            .expect("folder listing lock poisoned")
            .delimiter
    }

    pub(crate) fn attributes(&self) -> Vec<MailboxAttribute> {
        self.listing
            .read()
            .expect("folder listing lock poisoned")
            .attributes
            .clone()
    }

    /// Apply a same-name LIST/IDLE re-announcement to this entry in place.
    ///
    /// The announcement is the same UIDVALIDITY epoch, so cursor, MODSEQ
    /// cache, and last-seen stay untouched; only the LIST projection moves.
    /// Mutating through the shared `Arc` is the point: another task may be
    /// holding this same entry across an await and recording a cursor or a
    /// MODSEQ, and those writes must not be discarded by the refresh.
    pub(crate) fn refresh_listing(&self, info: &MailboxInfo) {
        *self.listing.write().expect("folder listing lock poisoned") =
            listing_from_info(info, self.rights.as_ref());
    }

    pub(crate) fn cursor(&self) -> Option<FolderCursor> {
        self.cursor
            .read()
            .expect("folder cursor lock poisoned")
            .clone()
    }

    pub(crate) fn set_cursor(&self, cursor: FolderCursor) {
        *self.cursor.write().expect("folder cursor lock poisoned") = Some(cursor);
    }

    pub(crate) fn modseq(&self, uidvalidity: u32, uid: u32) -> Option<u64> {
        let cache = self
            .modseq_by_uid
            .read()
            .expect("folder modseq lock poisoned");
        if cache.uidvalidity != Some(uidvalidity) {
            return None;
        }
        cache.by_uid.get(&uid).copied()
    }

    pub(crate) fn record_modseq(
        &self,
        uidvalidity: u32,
        uid: u32,
        modseq: u64,
    ) -> Result<(), crate::Error> {
        if uidvalidity == 0 {
            // The response-code decoder tolerates UIDVALIDITY 0 from some
            // servers. It is not a usable cache epoch, so do not cache it or
            // turn that tolerated response into a sync failure.
            return Ok(());
        }
        debug_assert_ne!(uid, 0, "MODSEQ cache update needs UID");
        if modseq == 0 {
            // The decoder tolerates MODSEQ 0 from servers as a sentinel. The
            // cache only informs a later STORE guard, so this value has no
            // useful protection to record and must not fail the sync walk.
            return Ok(());
        }
        let mut modseqs = self
            .modseq_by_uid
            .write()
            .map_err(|_| crate::Error::Internal("folder modseq lock poisoned".into()))?;
        if modseqs.uidvalidity != Some(uidvalidity) {
            modseqs.uidvalidity = Some(uidvalidity);
            modseqs.by_uid.clear();
        }
        modseqs.by_uid.insert(uid, modseq);
        modseqs.prune();
        Ok(())
    }

    pub(crate) fn clear_modseqs(&self, uidvalidity: u32, uids: &[u32]) {
        let mut modseqs = self
            .modseq_by_uid
            .write()
            .expect("folder modseq lock poisoned");
        if modseqs.uidvalidity != Some(uidvalidity) {
            return;
        }
        for uid in uids {
            modseqs.by_uid.remove(uid);
        }
    }

    pub(crate) fn mark_seen(&self) {
        *self
            .last_seen
            .lock()
            .expect("folder last_seen lock poisoned") = Some(Instant::now());
    }

    pub(crate) fn last_seen(&self) -> Option<Instant> {
        *self
            .last_seen
            .lock()
            .expect("folder last_seen lock poisoned")
    }
}

/// One shared/other-user folder discovered under a non-personal
/// namespace, carrying its owning mailbox identity (for membership tagging
/// and scoped recovery) plus the MYRIGHTS rights set observed at
/// discovery, when the server reported one.
pub(crate) struct SharedFolderEntry {
    pub(crate) info: MailboxInfo,
    /// The owning mailbox. For an other-user namespace (root `#user/`),
    /// this is the per-principal segment that follows the root in each
    /// folder's own path - `#user/alice/INBOX` -> `MailboxId("alice")` -
    /// so distinct users get distinct owners. For a shared namespace
    /// (root `#shared.`), all folders share one owner, the root minus its
    /// trailing delimiter -> `MailboxId("#shared")`.
    pub(crate) owner: bifrost_types::MailboxId,
    /// The parsed MYRIGHTS set, or `None` when the server did not report
    /// one (no ACL capability, or MYRIGHTS failed and discovery deferred
    /// to SELECT).
    pub(crate) rights: Option<crate::types::MailboxRights>,
    /// The NAMESPACE prefix this folder was enumerated under (`#user/`,
    /// `Shared/`, ...). Carried so `ingest_shared` can tell a folder that
    /// genuinely lives in a non-personal namespace from a candidate a
    /// misbehaving server echoed out of the personal namespace.
    pub(crate) namespace_prefix: String,
}

/// Whether a shared-namespace candidate outranks an identically-named entry
/// the personal LIST already produced.
///
/// True exactly when the candidate's path actually lies under the non-empty
/// NAMESPACE prefix it was enumerated with. That prefix is the server's own
/// declaration that the path is not personal, so the overlap is the personal
/// LIST over-reporting (RFC 2342 leaves `LIST "" "*"` free to include
/// non-personal namespaces, and several servers do), not a shared probe
/// leaking personal folders.
///
/// False for an empty prefix or a path outside it, which is the case a
/// reference/pattern-ignoring server produces: those candidates must not
/// demote a genuine personal folder.
///
/// Pure so the precedence rule is unit-pinnable without a live server.
pub(crate) fn shared_overrides_personal(name: &str, namespace_prefix: &str) -> bool {
    !namespace_prefix.is_empty() && name.starts_with(namespace_prefix)
}

/// The trailing path segment of a mailbox name under the server's own
/// hierarchy delimiter.
///
/// The single home for this rule. Both the `FolderRole` name fallback
/// (`pim.rs::folder_role`) and the Drafts probe that decides
/// `draft_create` (`capabilities.rs`) key roles off the leaf, and they
/// have to agree: a server where one sees `INBOX.Drafts` and the other
/// sees `Drafts` advertises a draft capability whose APPEND target then
/// fails to resolve. Two private copies of the split is how that drifts.
///
/// A `None` delimiter is LIST reporting a flat namespace, where the whole
/// name IS the leaf. We still split on `/` there, deliberately: it is the
/// long-standing behavior, and a server that reports NIL while genuinely
/// nesting under `/` is likelier than a flat server with a literal `/` in
/// a mailbox name. The cost of being wrong is a false role match on a
/// name like `Foo/Sent`; revisit if a real server hits it.
///
/// Pure so the delimiter rule is unit-pinnable without a live server.
pub(crate) fn leaf_name(name: &str, delimiter: Option<char>) -> &str {
    match delimiter {
        Some(delimiter) => name.rsplit_once(delimiter).map_or(name, |(_, leaf)| leaf),
        None => name.rsplit('/').next().unwrap_or(name),
    }
}

/// Whether a folder may be SELECTed, given LIST's own selectability
/// (`\Noselect` / `\NonExistent`) and the MYRIGHTS set discovery captured.
///
/// This is where the RFC 4314 read decision lands. Shared-folder discovery
/// used to enforce it by DROPPING an unreadable candidate from the shared
/// set, which silently demoted it: the personal `LIST "" "*"` is free to
/// echo the non-personal namespaces (RFC 2342), so the dropped path stayed
/// registered as that listing's bare entry, with no owner, no `Shared`
/// namespace, and no rights - a read-only share presenting as a writable
/// personal folder. Keeping the entry and clearing `selectable` instead
/// preserves the identity (owner, namespace, rights reach
/// `containers_list`) while still keeping the folder out of
/// `discover_cursor_scopes`, which filters on exactly this flag: no cursor
/// scope is created for a mailbox we cannot SELECT.
///
/// `None` rights means unreported (personal folder, no ACL capability, or a
/// MYRIGHTS failure that deferred to SELECT) and never gates.
///
/// Pure so the selectability decision is unit-pinnable without a server.
pub(crate) fn shared_folder_is_selectable(
    listed_selectable: bool,
    rights: Option<&crate::types::MailboxRights>,
) -> bool {
    listed_selectable && rights.is_none_or(crate::types::MailboxRights::can_read)
}

#[derive(Default)]
pub(crate) struct FolderRegistry {
    by_name: RwLock<HashMap<String, Arc<FolderEntry>>>,
}

impl FolderRegistry {
    pub(crate) fn from_list(folders: Vec<MailboxInfo>) -> Self {
        let registry = Self::default();
        registry.replace_all(folders);
        registry
    }

    /// Build the registry from the personal-root LIST plus the
    /// shared/other-user folders discovered under non-personal namespaces.
    /// Personal entries carry `shared_owner: None`; each shared entry
    /// carries `Some(owner)` so membership tagging and scoped revocation
    /// can route on the owning mailbox.
    pub(crate) fn from_lists(personal: Vec<MailboxInfo>, shared: Vec<SharedFolderEntry>) -> Self {
        let registry = Self::default();
        registry.replace_all(personal);
        registry.ingest_shared(shared);
        registry
    }

    /// Install shared/other-user folders, each tagged with its owning
    /// mailbox and MYRIGHTS set.
    ///
    /// The overlap rule is `shared_overrides_personal`: a candidate whose
    /// path lies under its own non-personal NAMESPACE prefix wins over an
    /// entry the personal `LIST "" "*"` already produced, because many
    /// servers (Dovecot among them) answer the unqualified personal LIST
    /// with the shared and other-user namespaces too. Skipping those
    /// overlaps - the prior rule - left the folder registered as a bare
    /// personal entry: no owner tag, no `Shared` namespace, and no rights,
    /// which is how a read-only shared folder became indistinguishable from
    /// a writable one downstream.
    ///
    /// A candidate NOT under its prefix is still skipped. That is the
    /// defense against a server that ignores the LIST reference/pattern
    /// split and answers a namespace LIST with personal folders: those
    /// must never demote a real personal INBOX to shared.
    pub(crate) fn ingest_shared(&self, shared: Vec<SharedFolderEntry>) {
        let mut map = self.by_name.write().expect("folder registry lock poisoned");
        for shared in shared {
            let name = shared.info.name.as_str().to_owned();
            if map.contains_key(&name)
                && !shared_overrides_personal(&name, &shared.namespace_prefix)
            {
                continue;
            }
            let entry = Arc::new(FolderEntry::from_mailbox_with_owner_rights(
                shared.info,
                Some(shared.owner),
                shared.rights,
            ));
            map.insert(name, entry);
        }
    }

    /// Replace the PERSONAL folder set, preserving the shared/other-user
    /// entries discovered under NAMESPACE at open.
    ///
    /// A mid-session re-LIST (`refresh_folders` after a create / rename /
    /// move / delete) only enumerates the personal root, so clearing the
    /// whole map would drop every shared folder along with its owner tag and
    /// MYRIGHTS - blanking the shared half of `containers_list` until the
    /// next account reopen. A personal candidate that collides with a
    /// retained shared entry does not overwrite it (the shared tagging is
    /// the more specific fact).
    ///
    /// A personal folder whose name survives the re-LIST keeps its EXISTING
    /// entry, refreshed in place, exactly as `apply_mailbox_event` does for
    /// a same-name re-announcement. Rebuilding it would zero its cursor and
    /// its (up to 50k-entry) MODSEQ cache on every folder CRUD, and orphan
    /// any `Arc<FolderEntry>` a concurrent task is holding across an await -
    /// that task's later cursor and MODSEQ writes would land on an entry no
    /// longer in the map. Only the names that disappeared are removed and
    /// the names that are new are added; the re-LIST is not a new
    /// UIDVALIDITY epoch, so nothing else about a surviving entry changes.
    pub(crate) fn replace_personal(&self, folders: Vec<MailboxInfo>) {
        let mut map = self.by_name.write().expect("folder registry lock poisoned");
        let listed: std::collections::HashSet<String> = folders
            .iter()
            .map(|info| info.name.as_str().to_owned())
            .collect();
        map.retain(|name, entry| entry.shared_owner.is_some() || listed.contains(name));
        for info in folders {
            let name = info.name.as_str().to_owned();
            if let Some(existing) = map.get(&name) {
                // A retained SHARED entry keeps its own listing: its
                // selectability is derived from MYRIGHTS, which a personal
                // LIST line carries nothing about.
                if existing.shared_owner.is_none() {
                    existing.refresh_listing(&info);
                }
                continue;
            }
            map.insert(name, Arc::new(FolderEntry::from_mailbox(info)));
        }
    }

    pub(crate) fn replace_all(&self, folders: Vec<MailboxInfo>) {
        let mut map = self.by_name.write().expect("folder registry lock poisoned");
        map.clear();
        for info in folders {
            let entry = Arc::new(FolderEntry::from_mailbox(info));
            map.insert(entry.name.as_str().to_owned(), entry);
        }
    }

    pub(crate) fn apply_mailbox_event(&self, info: MailboxInfo) {
        let name = info.name.as_str().to_owned();
        let old_name = info.old_name.as_ref().map(|name| name.as_str().to_owned());
        let deleted = info
            .attributes
            .iter()
            .any(|attr| matches!(attr, MailboxAttribute::NonExistent));
        let mut map = self.by_name.write().expect("folder registry lock poisoned");
        // Carry the shared-owner tag across a rename/recreate. A LIST/IDLE
        // `MailboxInfo` does not re-derive the owning mailbox (NAMESPACE
        // discovery does), so rebuilding the entry with `from_mailbox`
        // (owner `None`) would silently demote a renamed/recreated shared
        // folder to personal. A later SELECT denial on that folder would
        // then escalate account-wide (`NoPermission`) instead of routing
        // through the scoped `ScopeRevoked` quarantine. Source the owner
        // from the entry being superseded: the old name on a rename, the
        // same name on a recreate.
        // The rights set rides along with the owner tag for the same
        // reason: a LIST/IDLE `MailboxInfo` carries no MYRIGHTS, so
        // rebuilding without it would silently drop a shared folder's
        // read-only marking from `containers_list`.
        let superseded = old_name
            .as_ref()
            .and_then(|old| map.get(old))
            .or_else(|| map.get(&name));
        let inherited_owner = superseded.and_then(|entry| entry.shared_owner.clone());
        let inherited_rights = superseded.and_then(|entry| entry.rights.clone());
        if let Some(old_name) = old_name {
            map.remove(&old_name);
        }
        if deleted {
            map.remove(&name);
            return;
        }
        if !map.contains_key(&name) || info.old_name.is_some() {
            let entry = Arc::new(FolderEntry::from_mailbox_with_owner_rights(
                info,
                inherited_owner,
                inherited_rights,
            ));
            map.insert(name, entry);
        } else if let Some(existing) = map.get(&name) {
            // A same-name LIST/IDLE announcement is not a new UIDVALIDITY
            // epoch, but its attributes are live folder-lifecycle state.
            // In particular `\NoSelect` / `\NonExistent` and SPECIAL-USE
            // changes alter whether this folder is usable and how it is
            // exposed. Refresh in place: the entry keeps its identity, so
            // its cursor and MODSEQ cache survive, and so does any write a
            // task performs through an `Arc` it cloned before this event.
            existing.refresh_listing(&info);
        }
    }

    pub(crate) fn entries(&self) -> Vec<Arc<FolderEntry>> {
        self.by_name
            .read()
            .expect("folder registry lock poisoned")
            .values()
            .cloned()
            .collect()
    }

    pub(crate) fn get(&self, folder: &MailboxName) -> Option<Arc<FolderEntry>> {
        self.by_name
            .read()
            .expect("folder registry lock poisoned")
            .get(folder.as_str())
            .cloned()
    }

    pub(crate) fn set_cursor(&self, folder: &MailboxName, cursor: FolderCursor) {
        if let Some(entry) = self.get(folder) {
            entry.set_cursor(cursor);
        }
    }

    pub(crate) fn modseq(&self, folder: &MailboxName, uidvalidity: u32, uid: u32) -> Option<u64> {
        self.get(folder)
            .and_then(|entry| entry.modseq(uidvalidity, uid))
    }

    pub(crate) fn record_modseq(
        &self,
        folder: &MailboxName,
        uidvalidity: u32,
        uid: u32,
        modseq: u64,
    ) -> Result<(), crate::Error> {
        if let Some(entry) = self.get(folder) {
            return entry.record_modseq(uidvalidity, uid, modseq);
        }
        Ok(())
    }

    pub(crate) fn clear_modseqs(&self, folder: &MailboxName, uidvalidity: u32, uids: &[u32]) {
        if let Some(entry) = self.get(folder) {
            entry.clear_modseqs(uidvalidity, uids);
        }
    }

    pub(crate) fn mark_seen(&self, folder: &MailboxName) {
        if let Some(entry) = self.get(folder) {
            entry.mark_seen();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A shared-namespace discovery entry owned by `alice`, optionally
    /// carrying a MYRIGHTS wire string. Enumerated under `#user/`.
    fn shared_entry(name: &MailboxName, rights: Option<&str>) -> SharedFolderEntry {
        shared_entry_under(name, rights, "#user/")
    }

    fn shared_entry_under(
        name: &MailboxName,
        rights: Option<&str>,
        namespace_prefix: &str,
    ) -> SharedFolderEntry {
        SharedFolderEntry {
            info: MailboxInfo {
                name: name.clone(),
                ..Default::default()
            },
            owner: bifrost_types::MailboxId("alice".to_owned()),
            rights: rights.map(crate::types::MailboxRights::parse),
            namespace_prefix: namespace_prefix.to_owned(),
        }
    }

    #[test]
    fn compact_uid_set_roundtrips_and_diffs() {
        let set = CompactUidSet::from_uids([1, 2, 3, 7, 9, 10]);
        assert_eq!(set.to_uids(), vec![1, 2, 3, 7, 9, 10]);
        assert_eq!(
            set.ranges(),
            &[
                UidRange::range(1, 3),
                UidRange::single(7),
                UidRange::range(9, 10)
            ]
        );

        let newer = CompactUidSet::from_uids([2, 3, 4, 9]);
        let diff = set.diff(&newer);
        assert_eq!(diff.added, vec![4]);
        assert_eq!(diff.removed, vec![1, 7, 10]);
        assert_eq!(set.uid_count(), 6);
    }

    #[test]
    fn compact_uid_set_mutation_splits_and_merges_ranges() {
        let mut set = CompactUidSet::from_ranges(vec![UidRange::range(1, 500_000)]);
        assert!(set.remove(250_000));
        assert!(!set.contains(250_000));
        assert_eq!(set.ranges().len(), 2);
        assert!(!set.remove(250_000));

        assert!(set.insert(250_000));
        assert_eq!(set.ranges(), &[UidRange::range(1, 500_000)]);
        assert!(!set.insert(250_000));

        assert!(set.remove(1));
        assert!(set.remove(500_000));
        assert_eq!(set.ranges(), &[UidRange::range(2, 499_999)]);
    }

    // `UidRange::range(n, n)` and `UidRange::single(n)` are deliberately NOT
    // equal (see `types::uid_range`), and `from_uids` / `insert_range` both
    // normalise a one-element run to `single`. `remove` must land on the same
    // spelling: `CompactUidSet` derives `PartialEq` over the range vector, and
    // a mutated live set from `run_qresync` is what gets encoded into the
    // persisted CONDSTORE cursor. A `2:2` residue there would make the same
    // UID set compare unequal to - and serialise differently from - the set
    // rebuilt from the wire.
    #[test]
    fn compact_uid_set_remove_normalises_one_element_residue() {
        // Trimming the low end down to a single survivor.
        let mut set = CompactUidSet::from_uids([1, 2]);
        assert!(set.remove(1));
        assert_eq!(set, CompactUidSet::from_uids([2]));
        assert_eq!(set.ranges(), &[UidRange::single(2)]);

        // Trimming the high end down to a single survivor.
        let mut set = CompactUidSet::from_uids([1, 2]);
        assert!(set.remove(2));
        assert_eq!(set, CompactUidSet::from_uids([1]));
        assert_eq!(set.ranges(), &[UidRange::single(1)]);

        // Splitting from the middle, leaving a single survivor on each side.
        let mut set = CompactUidSet::from_uids([1, 2, 3]);
        assert!(set.remove(2));
        assert_eq!(set, CompactUidSet::from_uids([1, 3]));
        assert_eq!(set.ranges(), &[UidRange::single(1), UidRange::single(3)]);

        // Splitting with a multi-UID remainder on one side only.
        let mut set = CompactUidSet::from_uids([1, 2, 3, 4]);
        assert!(set.remove(2));
        assert_eq!(set, CompactUidSet::from_uids([1, 3, 4]));
        assert_eq!(set.ranges(), &[UidRange::single(1), UidRange::range(3, 4)]);
    }

    #[test]
    fn compact_uid_set_mutation_handles_empty_and_single_element_sets() {
        let mut set = CompactUidSet::default();
        assert!(!set.remove(1), "removing from an empty set reports absent");
        assert!(set.is_empty());

        assert!(set.insert(5));
        assert_eq!(set.ranges(), &[UidRange::single(5)]);
        assert!(!set.insert(5), "re-inserting reports already present");

        assert!(set.remove(5));
        assert!(set.is_empty(), "removing the only UID empties the set");
        assert_eq!(set.ranges(), &[]);

        // UID 0 is not an `nz-number` and must never enter the set.
        assert!(!set.insert(0));
        assert!(set.is_empty());
        assert!(!set.remove(0));
    }

    #[test]
    fn compact_uid_set_insert_merges_across_range_boundaries() {
        let mut set = CompactUidSet::from_uids([1, 2, 4, 5]);
        assert_eq!(
            set.ranges(),
            &[UidRange::range(1, 2), UidRange::range(4, 5)]
        );

        // The gap UID bridges two neighbours into one range.
        assert!(set.insert(3));
        assert_eq!(set.ranges(), &[UidRange::range(1, 5)]);
        assert_eq!(set.uid_count(), 5);

        // Extending past each endpoint keeps a single merged range.
        assert!(set.insert(6));
        assert_eq!(set.ranges(), &[UidRange::range(1, 6)]);
        assert!(set.insert(7));
        assert_eq!(set.ranges(), &[UidRange::range(1, 7)]);

        // A UID two past the end is disjoint, not adjacent.
        assert!(set.insert(9));
        assert_eq!(set.ranges(), &[UidRange::range(1, 7), UidRange::single(9)]);
        // ...and filling the hole merges it back in.
        assert!(set.insert(8));
        assert_eq!(set.ranges(), &[UidRange::range(1, 9)]);

        // A single UID adjacent below the first range extends it downward.
        let mut set = CompactUidSet::from_uids([5, 6]);
        assert!(set.insert(4));
        assert_eq!(set.ranges(), &[UidRange::range(4, 6)]);
        assert!(set.insert(1));
        assert_eq!(set.ranges(), &[UidRange::single(1), UidRange::range(4, 6)]);
    }

    // Every mutation path must leave the set indistinguishable from the same
    // UIDs fed through `from_uids`, since that is what membership, diffing and
    // cursor encoding all assume.
    #[test]
    fn compact_uid_set_mutations_agree_with_a_reference_set() {
        let mut set = CompactUidSet::default();
        let mut reference = std::collections::BTreeSet::new();

        // A deterministic walk that hits adjacency, splitting and re-merging.
        let script: [(bool, u32); 18] = [
            (true, 10),
            (true, 11),
            (true, 12),
            (true, 14),
            (true, 13),
            (false, 12),
            (true, 1),
            (true, 2),
            (false, 1),
            (false, 2),
            (true, 20),
            (false, 20),
            (true, 12),
            (false, 14),
            (false, 10),
            (true, 10),
            (false, 11),
            (false, 13),
        ];
        for (insert, uid) in script {
            if insert {
                assert_eq!(set.insert(uid), reference.insert(uid), "insert {uid}");
            } else {
                assert_eq!(set.remove(uid), reference.remove(&uid), "remove {uid}");
            }
            let expected = CompactUidSet::from_uids(reference.iter().copied());
            assert_eq!(
                set,
                expected,
                "after {}{uid}",
                if insert { '+' } else { '-' }
            );
            assert_eq!(set.uid_count(), reference.len());
            assert_eq!(set.to_uids(), reference.iter().copied().collect::<Vec<_>>());
            for uid in 1..25 {
                assert_eq!(
                    set.contains(uid),
                    reference.contains(&uid),
                    "contains {uid}"
                );
            }
        }
    }

    // The merge walks both range lists in order, so the edges worth pinning
    // are the ones where one side runs out before the other.
    #[test]
    fn compact_uid_set_diff_handles_exhausted_sides() {
        let empty = CompactUidSet::default();
        let some = CompactUidSet::from_uids([4, 5, 9]);

        let from_empty = empty.diff(&some);
        assert_eq!(from_empty.added, vec![4, 5, 9]);
        assert!(from_empty.removed.is_empty());

        let to_empty = some.diff(&empty);
        assert!(to_empty.added.is_empty());
        assert_eq!(to_empty.removed, vec![4, 5, 9]);

        assert_eq!(some.diff(&some), UidSetDiff::default());

        // Disjoint sets: every UID on each side must appear exactly once,
        // interleaved rather than concatenated.
        let disjoint = CompactUidSet::from_uids([1, 6, 7]);
        let diff = disjoint.diff(&some);
        assert_eq!(diff.added, vec![4, 5, 9]);
        assert_eq!(diff.removed, vec![1, 6, 7]);
    }

    #[test]
    fn compact_uid_set_diff_handles_max_uid_without_false_change() {
        let max = CompactUidSet::from_uids([u32::MAX]);
        assert_eq!(max.diff(&max), UidSetDiff::default());
        assert_eq!(CompactUidSet::default().diff(&max).added, vec![u32::MAX]);
        assert_eq!(max.diff(&CompactUidSet::default()).removed, vec![u32::MAX]);
    }

    /// `subtract_ranges` is hand-rolled range algebra whose whole point is to
    /// never expand an unchanged span, so a handful of examples cannot pin it:
    /// the interesting failures are partial overlaps, adjacency, and cover
    /// ranges that straddle a source-range boundary and must stay available to
    /// the next source range. Diff every pair against the obvious
    /// expand-and-compare answer over deterministic pseudo-random membership.
    #[test]
    fn compact_uid_set_diff_matches_set_semantics_over_generated_pairs() {
        fn naive(a: &CompactUidSet, b: &CompactUidSet) -> Vec<u32> {
            let cover: std::collections::BTreeSet<u32> = b.iter().collect();
            a.iter().filter(|uid| !cover.contains(uid)).collect()
        }

        let mut seed = 0x2545_F491_4F6C_DD1D_u64;
        let mut next = move || {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed
        };
        for _ in 0..400 {
            let mut left = Vec::new();
            let mut right = Vec::new();
            // A small universe with dense, streaky membership is what produces
            // long equal runs and boundary-straddling covers; uniform sparse
            // picks would almost never generate either.
            for uid in 1..=64_u32 {
                if next() % 3 != 0 {
                    left.push(uid);
                }
                if next() % 3 != 0 {
                    right.push(uid);
                }
            }
            let a = CompactUidSet::from_uids(left);
            let b = CompactUidSet::from_uids(right);
            let diff = a.diff(&b);
            assert_eq!(diff.added, naive(&b, &a), "added: {a:?} -> {b:?}");
            assert_eq!(diff.removed, naive(&a, &b), "removed: {a:?} -> {b:?}");
        }
    }

    // The cache is opportunistic, so the bound must hold without an
    // access-order structure: a full-mailbox inventory sweep leaves the
    // highest UIDs cached and nothing unbounded behind it.
    #[test]
    fn modseq_cache_prunes_to_the_target_keeping_the_newest_uids() {
        let info = MailboxInfo {
            name: MailboxName::new("INBOX").expect("valid mailbox"),
            ..Default::default()
        };
        let entry = FolderEntry::from_mailbox(info);
        let total = u32::try_from(MODSEQ_CACHE_CAPACITY).expect("fits") + 1;
        for uid in 1..=total {
            entry
                .record_modseq(11, uid, u64::from(uid))
                .expect("valid modseq");
        }
        assert_eq!(entry.modseq(11, total), Some(u64::from(total)));
        assert_eq!(
            entry.modseq(11, 1),
            None,
            "the oldest UID is the first evicted",
        );
        let survivors = (1..=total)
            .filter(|uid| entry.modseq(11, *uid).is_some())
            .count();
        assert_eq!(survivors, MODSEQ_CACHE_TARGET);
    }

    #[test]
    fn compact_uid_set_contains_probes_the_ranges() {
        let set = CompactUidSet::from_uids([1, 2, 3, 7, 9, 10]);
        for uid in set.to_uids() {
            assert!(set.contains(uid), "{uid} is in the set");
        }
        for uid in [0, 4, 5, 6, 8, 11, u32::MAX] {
            assert!(!set.contains(uid), "{uid} is not in the set");
        }
        assert!(!CompactUidSet::default().contains(1));
    }

    #[test]
    fn list_entry_special_use_and_selectability() {
        let info = MailboxInfo {
            name: MailboxName::new("Archive").expect("valid mailbox"),
            delimiter: Some('/'),
            attributes: vec![MailboxAttribute::Archive, MailboxAttribute::NoSelect],
            old_name: None,
            child_info: Vec::new(),
        };
        let entry = FolderEntry::from_mailbox(info);
        assert!(!entry.selectable());
    }

    #[test]
    fn folder_entry_tracks_modseq_by_uid() {
        let info = MailboxInfo {
            name: MailboxName::new("INBOX").expect("valid mailbox"),
            ..Default::default()
        };
        let entry = FolderEntry::from_mailbox(info);

        entry.record_modseq(11, 7, 99).expect("valid modseq");
        entry
            .record_modseq(0, 8, 100)
            .expect("zero UIDVALIDITY is not a cacheable epoch");
        entry
            .record_modseq(11, 8, 0)
            .expect("zero MODSEQ is ignored by the opportunistic cache");

        assert_eq!(entry.modseq(11, 7), Some(99));
        assert_eq!(entry.modseq(0, 8), None);
        assert_eq!(entry.modseq(11, 8), None);
        assert_eq!(entry.modseq(12, 7), None);

        entry.clear_modseqs(11, &[7]);
        assert_eq!(entry.modseq(11, 7), None);
    }

    #[test]
    fn folder_entry_drops_modseqs_from_old_uidvalidity_epoch() {
        let info = MailboxInfo {
            name: MailboxName::new("INBOX").expect("valid mailbox"),
            ..Default::default()
        };
        let entry = FolderEntry::from_mailbox(info);

        entry.record_modseq(11, 7, 99).expect("valid modseq");
        entry.record_modseq(12, 7, 100).expect("valid modseq");

        assert_eq!(entry.modseq(11, 7), None);
        assert_eq!(entry.modseq(12, 7), Some(100));
    }

    #[test]
    fn mailbox_delete_and_recreate_gets_fresh_entry() {
        let folder = MailboxName::new("Projects").expect("valid mailbox");
        let registry = FolderRegistry::from_list(vec![MailboxInfo {
            name: folder.clone(),
            ..Default::default()
        }]);
        let entry = registry.get(&folder).expect("folder entry");
        entry.record_modseq(11, 7, 99).expect("valid modseq");

        registry.apply_mailbox_event(MailboxInfo {
            name: folder.clone(),
            attributes: vec![MailboxAttribute::NonExistent],
            ..Default::default()
        });
        assert!(registry.get(&folder).is_none());

        registry.apply_mailbox_event(MailboxInfo {
            name: folder.clone(),
            ..Default::default()
        });
        let entry = registry.get(&folder).expect("recreated folder entry");
        assert_eq!(entry.modseq(11, 7), None);
        assert!(entry.cursor().is_none());
    }

    #[test]
    fn ingest_shared_does_not_overwrite_personal_entry() {
        let shared = MailboxName::new("Shared/INBOX").expect("valid mailbox");
        let overlap = MailboxName::new("INBOX").expect("valid mailbox");
        let registry = FolderRegistry::from_lists(
            vec![MailboxInfo {
                name: overlap.clone(),
                ..Default::default()
            }],
            vec![
                shared_entry(&overlap, None),
                shared_entry(&shared, Some("lr")),
            ],
        );

        // The personal INBOX must stay personal (no owner) despite an
        // overlapping shared candidate of the same name.
        let personal = registry.get(&overlap).expect("personal entry");
        assert!(personal.shared_owner.is_none());
        // The non-overlapping shared folder is still ingested.
        let ingested = registry.get(&shared).expect("shared entry");
        assert_eq!(
            ingested.shared_owner,
            Some(bifrost_types::MailboxId("alice".to_owned()))
        );
        // The discovery-time MYRIGHTS set is retained on the entry so
        // `containers_list` can project it.
        assert_eq!(
            ingested.rights,
            Some(crate::types::MailboxRights::parse("lr"))
        );
    }

    // RFC 2342 leaves `LIST "" "*"` free to include the non-personal
    // namespaces, and several servers do. When the SAME path comes back from
    // both the personal LIST and its own namespace LIST, the namespace
    // enumeration is authoritative: skipping the overlap left the folder
    // registered bare (no owner, no rights), which is exactly how a
    // read-only shared folder became indistinguishable from a writable one.
    #[test]
    fn shared_candidate_under_its_prefix_upgrades_an_overlapping_personal_entry() {
        let path = MailboxName::new("Shared/alice/Reports").expect("valid mailbox");
        let registry = FolderRegistry::from_lists(
            // The personal LIST already returned the shared path.
            vec![MailboxInfo {
                name: path.clone(),
                ..Default::default()
            }],
            vec![shared_entry_under(&path, Some("lr"), "Shared/")],
        );

        let entry = registry.get(&path).expect("entry present");
        assert_eq!(
            entry.shared_owner,
            Some(bifrost_types::MailboxId("alice".to_owned())),
            "a path under a declared shared namespace must carry its owner"
        );
        assert_eq!(
            entry.rights,
            Some(crate::types::MailboxRights::parse("lr")),
            "the MYRIGHTS set must survive the personal-LIST overlap"
        );
    }

    // The exact downstream shape: ONE grantee shares TWO folders under ONE
    // other-users prefix, one writable and one read-only, and the personal
    // LIST echoes both (RFC 2342). Earlier tests fed disjoint lists, so the
    // asymmetry never showed: discovery dropped the unreadable candidate,
    // the personal echo stayed, and the read-only share arrived at the
    // consumer as a writable PERSONAL folder. Both must carry owner,
    // namespace and rights; only the rights may differ.
    #[test]
    fn writable_and_read_only_shares_under_one_prefix_both_keep_owner_and_rights() {
        let writable = MailboxName::new("Shared/alice/Reports").expect("valid mailbox");
        let read_only = MailboxName::new("Shared/alice/Read Only").expect("valid mailbox");
        let registry = FolderRegistry::from_lists(
            vec![
                MailboxInfo {
                    name: writable.clone(),
                    ..Default::default()
                },
                MailboxInfo {
                    name: read_only.clone(),
                    ..Default::default()
                },
            ],
            vec![
                shared_entry_under(&writable, Some("lrswipkxte"), "Shared/"),
                shared_entry_under(&read_only, Some("lr"), "Shared/"),
            ],
        );

        let alice = Some(bifrost_types::MailboxId("alice".to_owned()));
        let w = registry.get(&writable).expect("writable entry");
        let r = registry.get(&read_only).expect("read-only entry");
        assert_eq!(w.shared_owner, alice);
        assert_eq!(
            r.shared_owner, alice,
            "a read-only share must carry the same owner as its writable sibling"
        );
        assert_eq!(
            w.rights,
            Some(crate::types::MailboxRights::parse("lrswipkxte"))
        );
        assert_eq!(
            r.rights,
            Some(crate::types::MailboxRights::parse("lr")),
            "the read-only rights set must reach the registry, not be discarded"
        );
        // Both are readable, so both stay selectable: rights are the ONLY
        // difference between the two entries.
        assert!(w.selectable());
        assert!(r.selectable());
    }

    // A share the grantee cannot read keeps its shared identity (so it can
    // never be mistaken for a personal folder) but is withheld from cursor
    // scoping, which filters on `selectable`.
    #[test]
    fn unreadable_share_stays_shared_but_unselectable() {
        let path = MailboxName::new("Shared/alice/No Read").expect("valid mailbox");
        let registry = FolderRegistry::from_lists(
            vec![MailboxInfo {
                name: path.clone(),
                ..Default::default()
            }],
            vec![shared_entry_under(&path, Some("l"), "Shared/")],
        );

        let entry = registry.get(&path).expect("entry present");
        assert_eq!(
            entry.shared_owner,
            Some(bifrost_types::MailboxId("alice".to_owned()))
        );
        assert_eq!(entry.rights, Some(crate::types::MailboxRights::parse("l")));
        assert!(
            !entry.selectable(),
            "an unreadable share must not become a cursor scope"
        );
    }

    #[test]
    fn selectability_gates_on_rights_only_when_rights_were_reported() {
        // Unreported rights never gate (personal folder, or no ACL).
        assert!(shared_folder_is_selectable(true, None));
        // `\Noselect` still wins regardless of rights.
        assert!(!shared_folder_is_selectable(
            false,
            Some(&crate::types::MailboxRights::parse("lrswipkxte"))
        ));
        // Read-only (`lr`) is selectable: `l`+`r` is the SELECT/FETCH pair.
        assert!(shared_folder_is_selectable(
            true,
            Some(&crate::types::MailboxRights::parse("lr"))
        ));
        // Lookup without read is visible but not selectable.
        assert!(!shared_folder_is_selectable(
            true,
            Some(&crate::types::MailboxRights::parse("l"))
        ));
    }

    #[test]
    fn shared_precedence_needs_the_path_to_be_under_the_prefix() {
        // The overlap case a namespace LIST genuinely outranks.
        assert!(shared_overrides_personal("Shared/alice/INBOX", "Shared/"));
        // A server that ignores the LIST reference/pattern split answers a
        // namespace probe with personal folders; those must never demote a
        // real personal INBOX.
        assert!(!shared_overrides_personal("INBOX", "Shared/"));
        // An empty prefix carries no claim at all.
        assert!(!shared_overrides_personal("INBOX", ""));
        // Textual prefix match is what the server itself declared, so a
        // sibling root that merely shares a leading substring still counts
        // only when the declared prefix matches.
        assert!(!shared_overrides_personal("SharedOther/x", "#user/"));
    }

    /// One leaf rule for both the `FolderRole` name fallback and the
    /// `draft_create` Drafts probe. If these two ever diverge again, a
    /// Courier/Dovecot `.`-delimited server advertises `draft_create`
    /// against a Drafts folder `role_folder` cannot then find.
    #[test]
    fn leaf_name_follows_the_servers_delimiter() {
        assert_eq!(leaf_name("INBOX.Drafts", Some('.')), "Drafts");
        assert_eq!(leaf_name("[Gmail]/Sent Mail", Some('/')), "Sent Mail");
        assert_eq!(
            leaf_name("INBOX", Some('.')),
            "INBOX",
            "a name with no delimiter occurrence is its own leaf"
        );
        assert_eq!(
            leaf_name("INBOX.Drafts", Some('/')),
            "INBOX.Drafts",
            "a dot is an ordinary character when the server delimits on slash"
        );
        assert_eq!(
            leaf_name("INBOX/Sent", None),
            "Sent",
            "NIL keeps the slash fallback; see the helper's doc comment"
        );
        assert_eq!(leaf_name("Drafts", None), "Drafts");
    }

    // A mid-session personal re-LIST (after a create / rename / move /
    // delete) must not take the shared entries down with it.
    #[test]
    fn replace_personal_retains_shared_entries_with_owner_and_rights() {
        let personal = MailboxName::new("INBOX").expect("valid mailbox");
        let shared = MailboxName::new("Shared/alice/Reports").expect("valid mailbox");
        let registry = FolderRegistry::from_lists(
            vec![MailboxInfo {
                name: personal.clone(),
                ..Default::default()
            }],
            vec![shared_entry_under(&shared, Some("lrswipkxte"), "Shared/")],
        );

        let created = MailboxName::new("Projects").expect("valid mailbox");
        registry.replace_personal(vec![
            MailboxInfo {
                name: personal.clone(),
                ..Default::default()
            },
            MailboxInfo {
                name: created.clone(),
                ..Default::default()
            },
        ]);

        assert!(
            registry.get(&created).is_some(),
            "new personal folder lands"
        );
        let retained = registry.get(&shared).expect("shared entry retained");
        assert_eq!(
            retained.shared_owner,
            Some(bifrost_types::MailboxId("alice".to_owned()))
        );
        assert_eq!(
            retained.rights,
            Some(crate::types::MailboxRights::parse("lrswipkxte")),
            "a personal re-LIST must not blank shared rights"
        );
    }

    // A mid-session personal re-LIST is not a new UIDVALIDITY epoch, so a
    // folder whose name survives it must survive it AS THE SAME ENTRY.
    // Rebuilding it zeroes the cursor and the MODSEQ cache on every folder
    // CRUD (turning the next round of STOREs unprotected), and orphans any
    // `Arc<FolderEntry>` a concurrent sync run is holding across an await:
    // that run's cursor and MODSEQ commits then land on an entry no longer
    // in the map. The attributes still refresh in place, and a name the
    // re-LIST dropped still leaves.
    #[test]
    fn replace_personal_preserves_surviving_entries_in_place() {
        let kept = MailboxName::new("INBOX").expect("valid mailbox");
        let gone = MailboxName::new("Old").expect("valid mailbox");
        let registry = FolderRegistry::from_list(vec![
            MailboxInfo {
                name: kept.clone(),
                ..Default::default()
            },
            MailboxInfo {
                name: gone.clone(),
                ..Default::default()
            },
        ]);

        let held = registry.get(&kept).expect("entry present");
        held.record_modseq(11, 7, 99).expect("valid modseq");
        held.set_cursor(FolderCursor::Basic {
            uidvalidity: 11,
            uidnext: 8,
            known_uids: CompactUidSet::from_uids([7]),
        });

        let created = MailboxName::new("Projects").expect("valid mailbox");
        registry.replace_personal(vec![
            MailboxInfo {
                name: kept.clone(),
                attributes: vec![MailboxAttribute::NoSelect],
                ..Default::default()
            },
            MailboxInfo {
                name: created.clone(),
                ..Default::default()
            },
        ]);

        let after = registry.get(&kept).expect("surviving entry");
        assert!(
            Arc::ptr_eq(&held, &after),
            "a surviving personal folder must keep its identity, not be rebuilt",
        );
        assert_eq!(
            after.modseq(11, 7),
            Some(99),
            "the MODSEQ cache must survive a personal re-LIST",
        );
        assert!(
            after.cursor().is_some(),
            "the cursor cache must survive a personal re-LIST",
        );
        assert!(
            !after.selectable(),
            "live LIST attributes still refresh in place",
        );
        // Writes through the pre-refresh Arc must still be visible: that is
        // the lost-update hazard a rebuild reintroduces.
        held.record_modseq(11, 8, 100).expect("valid modseq");
        assert_eq!(registry.get(&kept).expect("entry").modseq(11, 8), Some(100));

        assert!(registry.get(&created).is_some(), "a new folder is added");
        assert!(
            registry.get(&gone).is_none(),
            "a folder the re-LIST dropped is removed",
        );
    }

    #[test]
    fn mailbox_rename_removes_old_entry() {
        let old = MailboxName::new("Old").expect("valid mailbox");
        let new = MailboxName::new("New").expect("valid mailbox");
        let registry = FolderRegistry::from_list(vec![MailboxInfo {
            name: old.clone(),
            ..Default::default()
        }]);

        registry.apply_mailbox_event(MailboxInfo {
            name: new.clone(),
            old_name: Some(old.clone()),
            ..Default::default()
        });

        assert!(registry.get(&old).is_none());
        assert!(registry.get(&new).is_some());
    }

    #[test]
    fn rename_preserves_shared_owner_tag() {
        let old = MailboxName::new("Shared/alice/Old").expect("valid mailbox");
        let new = MailboxName::new("Shared/alice/New").expect("valid mailbox");
        let registry =
            FolderRegistry::from_lists(Vec::new(), vec![shared_entry(&old, Some("lrs"))]);

        registry.apply_mailbox_event(MailboxInfo {
            name: new.clone(),
            old_name: Some(old.clone()),
            ..Default::default()
        });

        let renamed = registry.get(&new).expect("renamed shared entry");
        assert_eq!(
            renamed.shared_owner,
            Some(bifrost_types::MailboxId("alice".to_owned())),
            "a renamed shared folder must keep its owner so a later SELECT denial quarantines",
        );
        assert_eq!(
            renamed.rights,
            Some(crate::types::MailboxRights::parse("lrs")),
            "a renamed shared folder must keep its MYRIGHTS projection",
        );
    }

    #[test]
    fn compact_uid_set_drops_zero_sorts_and_dedupes() {
        // UIDs are nz-number: a 0 is not a UID and must never reach a
        // range. Input order and duplicates must not survive either.
        let set = CompactUidSet::from_uids([9, 0, 2, 2, 1, 0, 3]);
        assert_eq!(set.to_uids(), vec![1, 2, 3, 9]);
        assert_eq!(set.ranges(), &[UidRange::range(1, 3), UidRange::single(9)]);
        assert_eq!(set.uid_count(), 4);

        assert!(CompactUidSet::from_uids([0, 0]).is_empty());
        assert!(CompactUidSet::default().is_empty());
        assert_eq!(CompactUidSet::default().uid_count(), 0);
    }

    #[test]
    fn compact_uid_set_from_ranges_normalizes_overlap_and_order() {
        // `from_ranges` is the cursor-decode entry point, so it must cope
        // with a set that was written out of order or with overlaps and
        // still produce one canonical merged range list.
        let set = CompactUidSet::from_ranges(vec![
            UidRange::range(5, 8),
            UidRange::single(1),
            UidRange::range(2, 6),
        ]);
        assert_eq!(set.ranges(), &[UidRange::range(1, 8)]);
        assert_eq!(set.uid_count(), 8);
    }

    #[test]
    fn compact_uid_set_diff_reports_both_directions() {
        let old = CompactUidSet::from_uids([1, 2, 3]);
        let new = CompactUidSet::from_uids([2, 3, 4]);
        let forward = old.diff(&new);
        assert_eq!(forward.added, vec![4]);
        assert_eq!(forward.removed, vec![1]);
        let backward = new.diff(&old);
        assert_eq!(backward.added, vec![1]);
        assert_eq!(backward.removed, vec![4]);
        // A set diffed against itself reports nothing.
        assert_eq!(old.diff(&old), UidSetDiff::default());
    }

    #[test]
    fn expand_range_covers_single_and_inclusive_range() {
        assert_eq!(expand_range(UidRange::single(4)), vec![4]);
        assert_eq!(expand_range(UidRange::range(4, 7)), vec![4, 5, 6, 7]);
        assert_eq!(expand_range(UidRange::range(4, 4)), vec![4]);
    }

    #[test]
    fn clear_modseqs_is_a_no_op_for_another_uidvalidity_epoch() {
        let info = MailboxInfo {
            name: MailboxName::new("INBOX").expect("valid mailbox"),
            ..Default::default()
        };
        let entry = FolderEntry::from_mailbox(info);
        entry.record_modseq(11, 7, 99).expect("valid modseq");

        // An expunge reported against a different epoch must not evict the
        // current epoch's cached MODSEQ (that would silently downgrade the
        // next STORE from UNCHANGEDSINCE-protected to unprotected).
        entry.clear_modseqs(12, &[7]);
        assert_eq!(entry.modseq(11, 7), Some(99));

        entry.clear_modseqs(11, &[7]);
        assert_eq!(entry.modseq(11, 7), None);
    }

    #[test]
    fn mark_seen_records_a_last_seen_instant() {
        let info = MailboxInfo {
            name: MailboxName::new("INBOX").expect("valid mailbox"),
            ..Default::default()
        };
        let entry = FolderEntry::from_mailbox(info);
        assert!(entry.last_seen().is_none());
        entry.mark_seen();
        assert!(entry.last_seen().is_some());
    }

    // A LIST/IDLE event for a folder we already know, with no OLDNAME and
    // no `\NonExistent`, is a re-announcement, not a new epoch: it retains
    // cursor/MODSEQ state but refreshes live attributes. Only a delete
    // followed by a create, or an explicit rename, installs a fresh entry.
    #[test]
    fn duplicate_create_event_refreshes_attributes_without_resetting_state() {
        let folder = MailboxName::new("Projects").expect("valid mailbox");
        let registry = FolderRegistry::from_list(vec![MailboxInfo {
            name: folder.clone(),
            ..Default::default()
        }]);
        let entry = registry.get(&folder).expect("folder entry");
        entry.record_modseq(11, 7, 99).expect("valid modseq");

        registry.apply_mailbox_event(MailboxInfo {
            name: folder.clone(),
            attributes: vec![MailboxAttribute::NoSelect, MailboxAttribute::Sent],
            ..Default::default()
        });

        let after = registry.get(&folder).expect("folder still registered");
        assert_eq!(
            after.modseq(11, 7),
            Some(99),
            "a re-announcement must not reset the MODSEQ cache"
        );
        assert!(!after.selectable(), "NoSelect must take effect immediately");
        assert_eq!(
            after.attributes(),
            vec![MailboxAttribute::NoSelect, MailboxAttribute::Sent],
            "SPECIAL-USE changes must refresh with the same announcement"
        );
    }

    /// The attribute refresh must mutate the entry every holder shares.
    ///
    /// Tasks clone `Arc<FolderEntry>` out of the registry and hold them
    /// across awaits (a sync run records MODSEQs and sets the folder cursor
    /// on the handle it took at the start). If a refresh copied that state
    /// into a replacement `Arc` and swapped the map entry, every write the
    /// holder made after the copy would land on an orphan and vanish - a
    /// just-recorded cursor lost, stale MODSEQ guards restored.
    ///
    /// No timing here: the handle is taken before the event and written
    /// after it, which is exactly the interleaving the swap loses.
    #[test]
    fn attribute_refresh_does_not_drop_writes_from_a_handle_taken_before_it() {
        let folder = MailboxName::new("Projects").expect("valid mailbox");
        let registry = FolderRegistry::from_list(vec![MailboxInfo {
            name: folder.clone(),
            ..Default::default()
        }]);
        // A task checks the folder out and is now mid-run.
        let held = registry.get(&folder).expect("folder entry");

        registry.apply_mailbox_event(MailboxInfo {
            name: folder.clone(),
            attributes: vec![MailboxAttribute::Sent],
            ..Default::default()
        });

        // The run finishes and commits its state through the handle it has.
        held.record_modseq(11, 7, 99).expect("valid modseq");
        held.set_cursor(FolderCursor::Basic {
            uidvalidity: 11,
            uidnext: 8,
            known_uids: CompactUidSet::from_uids([7]),
        });
        held.mark_seen();

        let after = registry.get(&folder).expect("folder still registered");
        assert_eq!(
            after.modseq(11, 7),
            Some(99),
            "a MODSEQ recorded through a pre-refresh handle must not be lost"
        );
        assert!(
            after.cursor().is_some(),
            "a cursor set through a pre-refresh handle must not be lost"
        );
        assert!(after.last_seen().is_some());
        assert_eq!(
            after.attributes(),
            vec![MailboxAttribute::Sent],
            "the refresh still has to land"
        );
    }

    #[test]
    fn recreate_preserves_shared_owner_tag() {
        let folder = MailboxName::new("Shared/alice/Proj").expect("valid mailbox");
        let registry = FolderRegistry::from_lists(Vec::new(), vec![shared_entry(&folder, None)]);

        // Recreate at the same name (fresh UIDVALIDITY epoch): a same-name
        // event with old_name set, or a contains-key replace path.
        registry.apply_mailbox_event(MailboxInfo {
            name: folder.clone(),
            old_name: Some(folder.clone()),
            ..Default::default()
        });

        let recreated = registry.get(&folder).expect("recreated shared entry");
        assert_eq!(
            recreated.shared_owner,
            Some(bifrost_types::MailboxId("alice".to_owned())),
        );
    }
}
