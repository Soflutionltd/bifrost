//! The DAV polling cursor: one snapshot shape, one codec, one diff, one
//! watermark page slicer.
//!
//! Neither CalDAV nor CardDAV has a change feed it can rely on. Both poll a
//! collection with a depth-1 PROPFIND, keep `(href, etag)` per member in an
//! opaque cursor, and derive `Created` / `Updated` / `Destroyed` by comparing
//! the new listing against the stored one. The two crates carried that whole
//! mechanism twice - the byte codec, the merge diff, the page slicer, the
//! inventory projection - differing only in the words `calendar` and
//! `addressbook`, and one of the recorded drift defects (a snapshot path fixed
//! on one side only) lived here.
//!
//! What stays in the crates is what genuinely differs: the magic bytes, the
//! name of the token (`sync-token` against `getctag`), and the error each maps
//! a malformed cursor onto.

use std::collections::HashSet;

use bifrost_types::{
    AccountError, AccountOperation, Change, Fingerprint, InventoryEntry, ObjectChange,
    ObjectChangeKind, ObjectId, ServerVersion,
};

use crate::error::{DavProtocol, local_error};

/// One member of a polled collection: the href that identifies it and the
/// validator that says whether it changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SnapshotEntry {
    pub uri: String,
    pub etag: Option<String>,
}

/// A cursor payload read back off the wire.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedSnapshot {
    pub collection_url: String,
    /// The collection-level token this protocol persists: CalDAV's
    /// `sync-token`, CardDAV's `getctag`.
    pub token: Option<String>,
    pub entries: Vec<SnapshotEntry>,
}

#[must_use]
pub fn object_change(uri: &str, kind: ObjectChangeKind) -> Change {
    Change::ObjectChange(ObjectChange {
        id: ObjectId(uri.to_string()),
        kind,
    })
}

/// Project one snapshot member onto an inventory entry. A member with no etag
/// reports `ServerVersion::Unavailable` rather than a fabricated version.
#[must_use]
pub fn inventory_entry(entry: &SnapshotEntry) -> InventoryEntry {
    InventoryEntry {
        id: ObjectId(entry.uri.clone()),
        memberships: Vec::new(),
        size: None,
        blob_id: None,
        fingerprint: Fingerprint {
            server_version: entry
                .etag
                .clone()
                .map(ServerVersion::ETag)
                .unwrap_or(ServerVersion::Unavailable),
            size: None,
            flags_hash: bifrost_types::canonical_flags_hash(std::iter::empty::<&str>()),
        },
        thread_id: None,
        message_id: None,
        references: Vec::new(),
        in_reply_to: None,
    }
}

/// Diff two href-sorted snapshots into object changes.
///
/// Suspected transient empty multistatus: a server returning zero hrefs against
/// a populated local snapshot would emit a `Destroyed` for every object and
/// wipe the consumer's store. Empty-versus-nonempty is therefore read as "no
/// observation", not "everything deleted".
///
/// Accepted cost, stated plainly because it is easy to re-file as a bug: a REAL
/// empty-out - a user deleting every member of the collection - is suppressed
/// along with the transient empty-207, on this poll AND on later ones, because
/// on the wire the two are identical. There is no signal that separates them,
/// so the choice is between never wrongly wiping a consumer's store and never
/// missing a genuine mass delete; both crates take the first, and the CalDAV
/// `sync-collection` lane (which is not exposed to a bare empty multistatus)
/// remains the path that does report one.
///
/// `current_failed` holds the hrefs the server reported *failed* within the 207
/// that produced `current`. A transiently-failed resource is not an absent one,
/// so it is never destroyed.
#[must_use]
pub fn diff_snapshots(
    previous: &[SnapshotEntry],
    current: &[SnapshotEntry],
    current_failed: &[String],
) -> Vec<Change> {
    if current.is_empty() && !previous.is_empty() {
        return Vec::new();
    }
    let failed: HashSet<&str> = current_failed.iter().map(String::as_str).collect();
    let mut changes = Vec::new();
    let mut left = 0;
    let mut right = 0;
    while left < previous.len() || right < current.len() {
        match (previous.get(left), current.get(right)) {
            (Some(old), Some(new)) if old.uri == new.uri => {
                if old.etag != new.etag {
                    changes.push(object_change(&new.uri, ObjectChangeKind::Updated));
                }
                left += 1;
                right += 1;
            }
            (Some(old), Some(new)) if old.uri < new.uri => {
                push_destroyed_unless_failed(&mut changes, &failed, &old.uri);
                left += 1;
            }
            (Some(_), Some(new)) => {
                changes.push(object_change(&new.uri, ObjectChangeKind::Created));
                right += 1;
            }
            (Some(old), None) => {
                push_destroyed_unless_failed(&mut changes, &failed, &old.uri);
                left += 1;
            }
            (None, Some(new)) => {
                changes.push(object_change(&new.uri, ObjectChangeKind::Created));
                right += 1;
            }
            (None, None) => break,
        }
    }
    changes
}

fn push_destroyed_unless_failed(changes: &mut Vec<Change>, failed: &HashSet<&str>, uri: &str) {
    if failed.contains(uri) {
        return;
    }
    changes.push(object_change(uri, ObjectChangeKind::Destroyed));
}

/// Whether a listing observed enough ground to advance its collection token.
/// Returned by the preservation helper so both DAV dialects use the same rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Observation {
    pub complete: bool,
    pub pending: Vec<String>,
}

impl Observation {
    /// An incomplete observation must be retried from the prior token. With no
    /// prior token, keep snapshot polling until the missing ground is observed.
    #[must_use]
    pub fn advance_token(&self, previous: Option<String>, next: Option<String>) -> Option<String> {
        if self.complete { next } else { previous }
    }
}

/// Carry forward the members this poll did not actually observe, so the next
/// diff compares like with like.
///
/// A wholly empty listing against a populated previous snapshot is no
/// observation at all and the previous entries are kept verbatim. Otherwise the
/// entries whose hrefs the server reported failed are re-inserted from the
/// previous snapshot, then the result is re-sorted and deduplicated on href -
/// the diff above depends on both. The returned observation also decides
/// whether the collection token can advance; preserving entries alone cannot
/// make checkpointing past a refused member safe.
pub fn preserve_unobserved_entries(
    previous: &[SnapshotEntry],
    current: &mut Vec<SnapshotEntry>,
    current_failed: &[String],
) -> Observation {
    let observation = Observation {
        complete: current_failed.is_empty() && (!current.is_empty() || previous.is_empty()),
        pending: current_failed.to_vec(),
    };
    if current.is_empty() && !previous.is_empty() {
        current.clear();
        current.extend_from_slice(previous);
        return observation;
    }
    let failed: HashSet<&str> = current_failed.iter().map(String::as_str).collect();
    current.extend(
        previous
            .iter()
            .filter(|entry| failed.contains(entry.uri.as_str()))
            .cloned(),
    );
    current.sort_by(|left, right| left.uri.cmp(&right.uri));
    current.dedup_by(|left, right| left.uri == right.uri);
    observation
}

/// Encode a snapshot into cursor bytes behind `magic`.
#[must_use]
pub fn encode_snapshot(
    magic: &[u8],
    collection_url: &str,
    token: Option<&str>,
    entries: &[SnapshotEntry],
) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(magic);
    write_string(&mut bytes, collection_url);
    write_option_string(&mut bytes, token);
    write_u32(&mut bytes, entries.len());
    for entry in entries {
        write_string(&mut bytes, &entry.uri);
        write_option_string(&mut bytes, entry.etag.as_deref());
    }
    bytes
}

/// Decode cursor bytes written by [`encode_snapshot`].
///
/// `label` names the protocol in the diagnostics ("CalDAV" / "CardDAV"); the
/// caller wraps the returned message in its own cursor error so the classified
/// `AccountError` stays crate-local.
///
/// Every length read is bounded against what remains: the entry count is
/// rejected outright when it exceeds one entry per five remaining bytes (the
/// smallest an entry can encode), so a corrupt count cannot make this allocate
/// against a four-byte lie. Trailing bytes are an error too - a payload this
/// did not fully consume is not a payload this understood.
pub fn decode_snapshot(magic: &[u8], label: &str, bytes: &[u8]) -> Result<DecodedSnapshot, String> {
    let mut input = bytes;
    if !input.starts_with(magic) {
        return Err(format!("{label} cursor magic mismatch"));
    }
    input = &input[magic.len()..];
    let collection_url = read_string(&mut input, label)?;
    let token = read_option_string(&mut input, label)?;
    let count = read_u32(&mut input, label)?;
    if count > input.len() / 5 {
        return Err(format!(
            "{label} cursor entry count exceeds remaining payload"
        ));
    }
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        entries.push(SnapshotEntry {
            uri: read_string(&mut input, label)?,
            etag: read_option_string(&mut input, label)?,
        });
    }
    if !input.is_empty() {
        return Err(format!("{label} cursor has trailing bytes"));
    }
    Ok(DecodedSnapshot {
        collection_url,
        token,
        entries,
    })
}

fn write_string(bytes: &mut Vec<u8>, value: &str) {
    write_u32(bytes, value.len());
    bytes.extend_from_slice(value.as_bytes());
}

fn write_option_string(bytes: &mut Vec<u8>, value: Option<&str>) {
    match value {
        Some(value) => {
            bytes.push(1);
            write_string(bytes, value);
        }
        None => bytes.push(0),
    }
}

fn write_u32(bytes: &mut Vec<u8>, value: usize) {
    let value = u32::try_from(value).unwrap_or(u32::MAX);
    bytes.extend_from_slice(&value.to_be_bytes());
}

fn read_string(input: &mut &[u8], label: &str) -> Result<String, String> {
    let len = read_u32(input, label)?;
    if input.len() < len {
        return Err(format!("{label} cursor string length exceeds payload"));
    }
    let value = String::from_utf8(input[..len].to_vec())
        .map_err(|error| format!("{label} cursor string is not UTF-8: {error}"))?;
    *input = &input[len..];
    Ok(value)
}

fn read_option_string(input: &mut &[u8], label: &str) -> Result<Option<String>, String> {
    let Some((tag, rest)) = input.split_first() else {
        return Err(format!("{label} cursor option tag is missing"));
    };
    *input = rest;
    match tag {
        0 => Ok(None),
        1 => read_string(input, label).map(Some),
        _ => Err(format!("{label} cursor option tag is invalid")),
    }
}

fn read_u32(input: &mut &[u8], label: &str) -> Result<usize, String> {
    let bytes = input
        .get(..4)
        .ok_or_else(|| format!("{label} cursor integer is truncated"))?;
    let bytes = <[u8; 4]>::try_from(bytes)
        .map_err(|error| format!("{label} cursor integer shape: {error}"))?;
    let value = u32::from_be_bytes(bytes);
    *input = &input[4..];
    Ok(value as usize)
}

/// Read a page watermark cursor: the sort key of the LAST item the previous
/// page served. An absent cursor is the start of the collection.
///
/// The watermark replaced an integer offset outright. An offset is only
/// meaningful against a materialized result set, so every continuation had to
/// re-fetch the whole collection in order to count into it; a key that lives in
/// the listing itself lets a page hydrate only its own members. It is also the
/// stronger cursor: an offset silently skips an item whenever something is
/// inserted before it between two pages, where a watermark cannot be moved
/// across by anything but a change of the key itself.
///
/// An empty payload is refused rather than read as "the beginning": every key
/// this pages on is a resource href, so an empty one is a corrupt cursor, and
/// silently restarting a pagination walk from item zero is the failure mode
/// with no symptom.
pub fn decode_watermark_cursor(
    cursor: Option<Vec<u8>>,
    operation: AccountOperation,
    protocol: DavProtocol,
) -> Result<Option<String>, AccountError> {
    let Some(cursor) = cursor else {
        return Ok(None);
    };
    let value = String::from_utf8(cursor).map_err(|error| {
        local_error(
            operation,
            format!("invalid {} page cursor: {error}", protocol.label()),
            protocol,
        )
    })?;
    if value.is_empty() {
        return Err(local_error(
            operation,
            format!("empty {} page cursor", protocol.label()),
            protocol,
        ));
    }
    Ok(Some(value))
}

/// Mint the cursor bytes for a watermark.
#[must_use]
pub fn encode_watermark_cursor(watermark: &str) -> Vec<u8> {
    watermark.as_bytes().to_vec()
}

/// One page taken out of a key-sorted sequence, plus the watermark that
/// continues it. `next_watermark` is absent when the page exhausted the
/// sequence.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PageSlice<T> {
    pub items: Vec<T>,
    pub next_watermark: Option<String>,
}

/// Take the page that follows `watermark` out of a key-sorted sequence.
///
/// `items` must already be sorted ASCENDING by `key`, and `key` must be stable
/// across polls: each continuation re-runs the remote request, and DAV
/// guarantees no ordering on a multistatus, so slicing raw response order would
/// let an unchanged result set come back permuted between page one and page two
/// and serve some members twice while never serving others at all. Both DAV
/// crates key on the resource href (CalDAV through the recurrence-qualified
/// event id, CardDAV through the native id), which is the only key the depth-1
/// listing carries and the only one a member update cannot move.
///
/// A member inserted BEFORE the watermark between two pages is therefore not
/// re-served, and one inserted after it is served on a later page - the
/// exactly-once property an integer offset could not hold.
///
/// A zero page size is an exhausted page, not a page of nothing that still
/// points at itself: re-emitting the current watermark whenever results exist
/// gives a consumer that follows `next_cursor` an infinite loop that never
/// advances and never delivers an item.
#[must_use]
pub fn slice_after_watermark<T>(
    items: Vec<T>,
    watermark: Option<&str>,
    page_size: usize,
    key: impl Fn(&T) -> &str,
) -> PageSlice<T> {
    if page_size == 0 {
        return PageSlice {
            items: Vec::new(),
            next_watermark: None,
        };
    }
    let mut remaining = match watermark {
        Some(watermark) => items
            .into_iter()
            .filter(|item| key(item) > watermark)
            .collect(),
        None => items,
    };
    let more = remaining.len() > page_size;
    remaining.truncate(page_size);
    let next_watermark = if more {
        remaining.last().map(|item| key(item).to_string())
    } else {
        None
    };
    PageSlice {
        items: remaining,
        next_watermark,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(uri: &str, etag: Option<&str>) -> SnapshotEntry {
        SnapshotEntry {
            uri: uri.to_string(),
            etag: etag.map(str::to_string),
        }
    }

    #[test]
    fn only_complete_observations_advance_tokens() {
        let previous = vec![entry("/c/a", Some("old"))];
        for (mut current, failed) in [
            (Vec::new(), Vec::new()),
            (vec![entry("/c/b", Some("new"))], vec!["/c/a".to_string()]),
        ] {
            let observation = preserve_unobserved_entries(&previous, &mut current, &failed);
            assert!(!observation.complete);
            assert_eq!(observation.pending, failed);
            assert_eq!(
                observation.advance_token(Some("old".into()), Some("new".into())),
                Some("old".into())
            );
            assert_eq!(observation.advance_token(None, Some("new".into())), None);
        }
        let mut current = previous.clone();
        let observation = preserve_unobserved_entries(&previous, &mut current, &[]);
        assert_eq!(
            observation.advance_token(Some("old".into()), Some("new".into())),
            Some("new".into())
        );
    }

    #[test]
    fn a_snapshot_round_trips_through_the_cursor_codec() {
        let entries = vec![entry("/c/one", Some("a")), entry("/c/two", None)];
        let bytes = encode_snapshot(b"MAGIC1", "/c/", Some("token-1"), &entries);

        let decoded = decode_snapshot(b"MAGIC1", "CalDAV", &bytes).expect("valid cursor");
        assert_eq!(decoded.collection_url, "/c/");
        assert_eq!(decoded.token.as_deref(), Some("token-1"));
        assert_eq!(decoded.entries, entries);
    }

    #[test]
    fn a_corrupt_entry_count_is_refused_rather_than_allocated() {
        let mut bytes = encode_snapshot(b"MAGIC1", "/c/", None, &[]);
        let tail = bytes.len() - 4;
        bytes[tail..].copy_from_slice(&u32::MAX.to_be_bytes());

        let error = decode_snapshot(b"MAGIC1", "CardDAV", &bytes).expect_err("refused");
        assert!(
            error.contains("entry count exceeds remaining payload"),
            "{error}"
        );
    }

    #[test]
    fn a_wholly_empty_listing_is_no_observation_rather_than_a_mass_delete() {
        let previous = vec![entry("/c/one", Some("a"))];
        assert!(diff_snapshots(&previous, &[], &[]).is_empty());
    }

    #[test]
    fn a_failed_href_is_preserved_rather_than_destroyed() {
        let previous = vec![entry("/c/one", Some("a")), entry("/c/two", Some("b"))];
        let current = vec![entry("/c/two", Some("b"))];

        assert!(diff_snapshots(&previous, &current, &["/c/one".to_string()]).is_empty());
        let unguarded = diff_snapshots(&previous, &current, &[]);
        assert!(matches!(
            unguarded.as_slice(),
            [Change::ObjectChange(ObjectChange {
                id,
                kind: ObjectChangeKind::Destroyed,
            })] if id.0 == "/c/one"
        ));
    }

    /// A zero page size is an exhausted page, not a page of nothing that still
    /// points at itself: re-emitting the current watermark whenever results
    /// exist gives a consumer that follows `next_cursor` an infinite loop that
    /// never advances and never delivers an item.
    #[test]
    fn a_zero_page_size_is_exhausted_rather_than_self_pointing() {
        let slice = slice_after_watermark(hrefs(&["a", "b", "c"]), None, 0, String::as_str);
        assert!(slice.items.is_empty());
        assert!(slice.next_watermark.is_none());

        // The loop shape is the one with a cursor: a consumer that follows
        // `next_cursor` and asks for zero items must not be handed its own
        // watermark back, which advances nothing and delivers nothing forever.
        let continued =
            slice_after_watermark(hrefs(&["a", "b", "c"]), Some("a"), 0, String::as_str);
        assert!(continued.items.is_empty());
        assert!(continued.next_watermark.is_none());
    }

    fn keys(items: &[String]) -> Vec<&str> {
        items.iter().map(String::as_str).collect()
    }

    fn hrefs(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    #[test]
    fn a_watermark_cursor_round_trips_through_its_codec() {
        let bytes = encode_watermark_cursor("/c/two.ics");
        assert_eq!(
            decode_watermark_cursor(
                Some(bytes),
                AccountOperation::EventsInRange,
                DavProtocol::CalDav
            )
            .expect("valid cursor"),
            Some("/c/two.ics".to_string())
        );
        assert_eq!(
            decode_watermark_cursor(None, AccountOperation::EventsInRange, DavProtocol::CalDav)
                .expect("absent cursor"),
            None
        );
        // An empty payload is a corrupt cursor, not a silent restart from the
        // first member of the collection.
        assert!(
            decode_watermark_cursor(
                Some(Vec::new()),
                AccountOperation::EventsInRange,
                DavProtocol::CalDav
            )
            .is_err()
        );
        assert!(
            decode_watermark_cursor(
                Some(vec![0xff, 0xfe]),
                AccountOperation::ContactsList,
                DavProtocol::CardDav
            )
            .is_err()
        );
    }

    #[test]
    fn a_watermark_page_continues_from_the_last_key_served() {
        let first = slice_after_watermark(hrefs(&["a", "b", "c"]), None, 2, String::as_str);
        assert_eq!(keys(&first.items), vec!["a", "b"]);
        assert_eq!(first.next_watermark.as_deref(), Some("b"));

        let second = slice_after_watermark(
            hrefs(&["a", "b", "c"]),
            first.next_watermark.as_deref(),
            2,
            String::as_str,
        );
        assert_eq!(keys(&second.items), vec!["c"]);
        assert_eq!(second.next_watermark, None);
    }

    /// The property the watermark exists for. Against an integer offset, `aa`
    /// arriving between the two pages pushes `b` behind the offset and it is
    /// never served; the watermark cannot be crossed by an insertion.
    #[test]
    fn an_insert_before_the_watermark_does_not_displace_a_later_page() {
        let first = slice_after_watermark(hrefs(&["a", "b", "c"]), None, 2, String::as_str);
        assert_eq!(keys(&first.items), vec!["a", "b"]);

        let second = slice_after_watermark(
            hrefs(&["a", "aa", "b", "c"]),
            first.next_watermark.as_deref(),
            2,
            String::as_str,
        );
        assert_eq!(keys(&second.items), vec!["c"]);
        // And one inserted AFTER the watermark is served on the later page
        // rather than lost.
        let with_insert = slice_after_watermark(
            hrefs(&["a", "b", "bb", "c"]),
            first.next_watermark.as_deref(),
            2,
            String::as_str,
        );
        assert_eq!(keys(&with_insert.items), vec!["bb", "c"]);
    }

    /// A watermark past every key is the end of the walk, not a page that
    /// points at itself.
    #[test]
    fn a_watermark_past_every_key_is_an_empty_final_page() {
        let slice = slice_after_watermark(hrefs(&["a", "b", "c"]), Some("z"), 2, String::as_str);
        assert!(slice.items.is_empty());
        assert!(slice.next_watermark.is_none());
    }
}
