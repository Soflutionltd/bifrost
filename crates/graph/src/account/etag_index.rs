use crate::etag::GraphETag;
use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

/// Entity tags are a best-effort mutation optimization. Retaining one per
/// object forever turns a long-running high-churn account into an unbounded
/// cache, so keep only a fixed working set.
pub(crate) const ETAG_INDEX_MAX_ENTRIES: usize = 10_000;

/// An entity tag plus the recency ticket that orders it in `EtagIndex::order`.
struct EtagEntry {
    etag: GraphETag,
    seq: u64,
}

/// Bounded LRU over entity tags.
///
/// Every operation is one hash lookup plus one or two `BTreeMap` node
/// operations - no scan of the cache. That matters because the write lock
/// guarding this index is taken once per hydrated page and once per mutation
/// preflight: a recency policy that walked the entries would make a full
/// inventory pass cost `messages * capacity` comparisons, serialized, which
/// is a worse trade than the unbounded map it replaced.
#[derive(Default)]
pub(crate) struct EtagIndex {
    /// Keys are `Arc<str>` so a recency ticket can name an entry without a
    /// second copy of the (long, base64url) Graph id.
    entries: HashMap<Arc<str>, EtagEntry>,
    /// Recency tickets, oldest first. Exactly one row per `entries` row,
    /// keyed by that row's current `seq`, so eviction is `pop_first`.
    order: BTreeMap<u64, Arc<str>>,
    next_seq: u64,
}

/// Reads learn a validator; writes consume the old one even without a new
/// validator. Failed preconditions and vanished targets invalidate it.
pub(crate) enum EtagObservation {
    Read(GraphETag),
    Written(Option<GraphETag>),
    Invalidated,
}

impl EtagIndex {
    pub(crate) fn observe(&mut self, id: &str, observation: EtagObservation) {
        match observation {
            EtagObservation::Read(etag) | EtagObservation::Written(Some(etag)) => {
                self.insert(id.to_owned(), etag);
            }
            EtagObservation::Written(None) | EtagObservation::Invalidated => self.remove(id),
        }
    }

    pub(crate) fn insert(&mut self, id: String, etag: GraphETag) {
        // `remove_entry` hands back the owned key, so re-caching an id
        // already present does not reallocate it.
        let (key, previous) = match self.entries.remove_entry(id.as_str()) {
            Some((key, entry)) => (key, Some(entry.seq)),
            None => {
                self.evict_to_capacity();
                (Arc::from(id), None)
            }
        };
        let seq = self.promote(Arc::clone(&key), previous);
        self.entries.insert(key, EtagEntry { etag, seq });
    }

    /// Reads an entity tag AND marks it hot. Mutation preflight is the only
    /// reader, so an id being read is an id about to carry an `If-Match`.
    pub(crate) fn get(&mut self, id: &str) -> Option<GraphETag> {
        let (key, entry) = self.entries.remove_entry(id)?;
        let seq = self.promote(Arc::clone(&key), Some(entry.seq));
        let etag = entry.etag.clone();
        self.entries.insert(
            key,
            EtagEntry {
                etag: entry.etag,
                seq,
            },
        );
        Some(etag)
    }

    pub(crate) fn remove(&mut self, id: &str) {
        if let Some(entry) = self.entries.remove(id) {
            self.order.remove(&entry.seq);
        }
    }

    #[cfg(test)]
    fn len(&self) -> usize {
        self.entries.len()
    }

    #[cfg(test)]
    fn contains_key(&self, id: &str) -> bool {
        self.entries.contains_key(id)
    }

    #[cfg(test)]
    fn order_len(&self) -> usize {
        self.order.len()
    }

    /// Issues `key` a fresh recency ticket, retiring `previous` if it had
    /// one. The caller must store the returned `seq` on the entry, which is
    /// what keeps `order` and `entries` one-to-one.
    fn promote(&mut self, key: Arc<str>, previous: Option<u64>) -> u64 {
        if let Some(previous) = previous {
            self.order.remove(&previous);
        }
        let seq = self.next_seq;
        // A u64 ticket space is not exhaustible by any real account: at one
        // cache write per nanosecond it lasts ~585 years.
        self.next_seq = self.next_seq.wrapping_add(1);
        self.order.insert(seq, key);
        seq
    }

    /// Drops the coldest entries until one more will fit.
    fn evict_to_capacity(&mut self) {
        while self.entries.len() >= ETAG_INDEX_MAX_ENTRIES {
            let Some((_, coldest)) = self.order.pop_first() else {
                break;
            };
            self.entries.remove(&coldest);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn etag_cache_evicts_the_least_recently_used_entry() {
        let mut cache = EtagIndex::default();
        for index in 0..=ETAG_INDEX_MAX_ENTRIES {
            cache.insert(
                format!("id-{index}"),
                GraphETag::from_odata_annotation(&format!("etag-{index}")),
            );
        }
        assert_eq!(cache.len(), ETAG_INDEX_MAX_ENTRIES);
        assert!(!cache.contains_key("id-0"));
        assert!(cache.contains_key("id-10000"));

        // A read refreshes the recency position. On the next insertion the
        // oldest untouched entry must go, not the hot one.
        assert_eq!(cache.get("id-1").as_deref(), Some("etag-1"));
        cache.insert(
            "new".to_string(),
            GraphETag::from_odata_annotation("etag-new"),
        );
        assert!(cache.contains_key("id-1"));
        assert!(!cache.contains_key("id-2"));
        assert!(cache.contains_key("new"));
    }

    /// Eviction reads the recency order without consulting the entry map, so
    /// the two must stay one-to-one: a re-insert, a hit, and a removal each
    /// have to retire the ticket they replace. A leaked ticket would evict a
    /// key that is no longer there and let the cache grow past its bound; a
    /// missing one would make an entry immortal.
    #[test]
    fn etag_cache_keeps_one_recency_ticket_per_entry() {
        let mut cache = EtagIndex::default();
        cache.insert("a".to_string(), GraphETag::from_odata_annotation("etag-a"));
        cache.insert("b".to_string(), GraphETag::from_odata_annotation("etag-b"));

        // Re-caching an id updates it in place.
        cache.insert("a".to_string(), GraphETag::from_odata_annotation("etag-a2"));
        assert_eq!(cache.len(), 2);
        assert_eq!(cache.order_len(), 2);
        assert_eq!(cache.get("a").as_deref(), Some("etag-a2"));
        assert_eq!(cache.order_len(), 2);

        cache.remove("a");
        assert_eq!(cache.len(), 1);
        assert_eq!(cache.order_len(), 1);
        // Removing an absent id is a no-op, not a ticket leak.
        cache.remove("a");
        assert_eq!(cache.order_len(), 1);

        // The oldest surviving ticket still names a live entry: refill to
        // capacity and only "b" (never touched again) may be evicted.
        for index in 0..ETAG_INDEX_MAX_ENTRIES {
            cache.insert(
                format!("fill-{index}"),
                GraphETag::from_odata_annotation("etag"),
            );
        }
        assert_eq!(cache.len(), ETAG_INDEX_MAX_ENTRIES);
        assert_eq!(cache.order_len(), ETAG_INDEX_MAX_ENTRIES);
        assert!(!cache.contains_key("b"));
        assert!(cache.contains_key("fill-0"));
    }
}
