//! Consumer-acknowledged completion marker publication.
use crate::delivery::{EventScope, MultiplexerEvent};
use crate::engine::backfill_bound::BackfillBound;
use bifrost_types::{
    BackfillCheckpoint, Batch, Checkpoint, CursorScope, PageBoundary, Partition, SyncEvent,
};
use std::sync::Arc;
use std::time::Duration;

/// Engine-owned durable backfill key. Existing key spellings remain readable,
/// and unknown keys retain their original bytes.
/// Inventory grammar is delegated to the types-owned codec; only completion
/// markers and their legacy proof rules belong to this engine layer.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum BackfillKey {
    Full,
    Time {
        from: Option<i64>,
        to: Option<i64>,
    },
    Uid {
        uid_validity: u32,
        from: u64,
        to: u64,
    },
    Page {
        from: u32,
        to: u32,
    },
    Complete,
    LegacyComplete,
    Opaque(Partition),
}

impl BackfillKey {
    #[must_use]
    pub fn decode(partition: &Partition) -> Self {
        let Ok(text) = std::str::from_utf8(&partition.0) else {
            return Self::Opaque(partition.clone());
        };
        match text {
            "complete" => return Self::LegacyComplete,
            "complete:v2" => return Self::Complete,
            _ => {}
        }
        use bifrost_types::wire::partition::InventoryKey;
        match InventoryKey::decode(partition) {
            InventoryKey::Full => Self::Full,
            InventoryKey::Time { from, to } => Self::Time { from, to },
            InventoryKey::Uid {
                uid_validity,
                from,
                to,
            } => Self::Uid {
                uid_validity,
                from,
                to,
            },
            InventoryKey::Page { from, to } => Self::Page { from, to },
            _ => Self::Opaque(partition.clone()),
        }
    }

    #[must_use]
    pub fn encode(&self) -> Partition {
        use bifrost_types::wire::partition::InventoryKey;
        let inventory = match self {
            Self::Full => InventoryKey::Full,
            Self::Time { from, to } => InventoryKey::Time {
                from: *from,
                to: *to,
            },
            Self::Uid {
                uid_validity,
                from,
                to,
            } => InventoryKey::Uid {
                uid_validity: *uid_validity,
                from: *from,
                to: *to,
            },
            Self::Page { from, to } => InventoryKey::Page {
                from: *from,
                to: *to,
            },
            Self::Complete => return Partition(b"complete:v2".to_vec()),
            Self::LegacyComplete => return Partition(b"complete".to_vec()),
            Self::Opaque(partition) => return partition.clone(),
        };
        inventory.encode()
    }

    /// Whether this key names a completion marker, including legacy markers.
    #[must_use]
    pub fn is_completion(&self) -> bool {
        matches!(self, Self::Complete | Self::LegacyComplete)
    }

    /// Legacy empty-plan markers require one inventory recheck. Counts on new
    /// markers are ordinary counts, never an ordering discriminator.
    #[must_use]
    pub fn proves_completion(&self, items_done: u64) -> bool {
        matches!(self, Self::Complete) || matches!(self, Self::LegacyComplete) && items_done > 1
    }
}

/// Broadcast a durable backfill-completion marker for a scope. The marker
/// is a synthetic empty `Batch` carrying a `BackfillCheckpoint` on the
/// `completion_partition` sentinel key; it flows through the consumer-ack
/// path exactly like a page batch, so the store only records completion
/// after the consumer has durably persisted every page. A crash before the
/// marker is acked therefore re-walks rather than recording a false "done".
///
/// `backfill_checkpoint_is_later` selects completion by durable key kind.
/// Both progress fields report `total_seen`. Stores must use the typed resume
/// ordering: inflating a count cannot beat a larger row from an older walk.
#[allow(clippy::too_many_arguments)]
pub(crate) async fn emit_backfill_complete(
    scope: &CursorScope,
    total_seen: crate::backfill::scope_walk::WalkTotal,
    publisher: &crate::delivery::PagePublisher,
    bound: &BackfillBound,
    undelivered_before: crate::cursor::UndeliveredMark,
    fence_at_walk_start: crate::cursor::ScopeFence,
) -> MarkerOutcome {
    let control = &publisher.control;
    let shutdown = &publisher.shutdown;
    if !publisher.shares_bound_ledger(bound) {
        return MarkerOutcome::Withheld;
    }
    let _activity = loop {
        if !control.wait_until_running(shutdown).await {
            return MarkerOutcome::ShuttingDown;
        }
        if let Some(activity) = control.begin_activity() {
            break activity;
        }
    };
    let marker = BackfillCheckpoint {
        scope: scope.clone(),
        partition: BackfillKey::Complete.encode(),
        progress_marker: None,
        progress: total_seen.completion_progress(),
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(crate::cursor::ENGINE_VERSION),
    };
    let batch: Batch<bifrost_types::Change> = Batch {
        items: Vec::new(),
        page_boundary: PageBoundary::Final,
        server_latency: Duration::ZERO,
        bytes_in: 0,
        checkpoint: Some(Checkpoint::Backfill(marker.clone())),
    };
    let expected = Checkpoint::Backfill(marker);
    // The sentinel is a backfill publication like any other and waits on the
    // same bound. It takes NO scheduler admission, and must not: it does no wire
    // work, and it is emitted from here - after the partition pass has already
    // handed its permit back - so a wait that re-acquired admission on the wake
    // would leave that permit held for the rest of the attachment, starving
    // polling, push reconciliation and the next scope's barrier query on a
    // one-permit budget.
    // The fence to beat is the one this WALK started under, not one read here.
    // A reset can close between the walk's last page and the provider stream's
    // terminal `Done` - the stream is not cancelled, it just ends - and a
    // snapshot taken at this point is already the replacement incarnation's, so
    // the comparison below would be a walk checking itself against a fence it
    // adopted from a scope it never enumerated.
    let fence_before = fence_at_walk_start;
    if bound.coverage().scope_fence(scope) != fence_before {
        tracing::warn!(
            target: "bifrost.sync.backfill",
            scope = ?scope,
            "the scope was reset during this walk; withholding its completion marker"
        );
        return MarkerOutcome::Withheld;
    }
    if bound.wait_for_capacity().await.is_err() {
        return MarkerOutcome::ShuttingDown;
    }
    // Same revalidation the page path does, and the stakes are higher here: a
    // completion marker published against a reset scope suppresses the
    // replacement incarnation's entire inventory walk on the next attach.
    if bound.coverage().scope_fence(scope) != fence_before {
        tracing::warn!(
            target: "bifrost.sync.backfill",
            scope = ?scope,
            "scope was reset while its completion marker waited on the bound; \
             withholding the marker"
        );
        return MarkerOutcome::Withheld;
    }
    // And re-validate the WALK, not only the scope's rows. The wait above is
    // indefinite, and the caller's "every page of this walk reached somebody"
    // reading was taken before it: a final page still unacknowledged when the
    // marker parked can be swept undelivered by a departing receiver, whose
    // sweep is also what frees the capacity this wait is parked on. Publishing
    // on that wake records a durable completion for a walk that lost a page one
    // instant earlier, and the replacement consumer acknowledges it in good
    // faith. The page is gone for the life of the attachment either way; what
    // this refuses is making it permanent.
    if bound.coverage().undelivered_watermark(scope) != undelivered_before {
        tracing::warn!(
            target: "bifrost.sync.backfill",
            scope = ?scope,
            "a page of this walk reached no consumer while its completion marker waited \
             on the bound; withholding the marker so the scope is re-walked"
        );
        return MarkerOutcome::Withheld;
    }
    // Register before publishing so a fast consumer ack cannot land
    // before the entry exists and leave it outstanding forever.
    // Completion eligibility stays tied to THIS walk right up to the
    // acknowledgement, so the reading is stamped into the publication's RECEIPT
    // as it is minted. The checks above are point-in-time and the marker is
    // durable: with spare capacity it reaches every receiver at once, and a
    // receiver still holding an earlier unacknowledged page can depart
    // afterwards - the sweep retires that page while a replacement, which had
    // the marker in its ring all along, acknowledges it. The writer compares
    // this reading against the scope's watermark again before it writes.
    let event = MultiplexerEvent {
        scope: EventScope::Cursor(scope.clone()),
        event: Arc::new(SyncEvent::Batch(batch)),
        checkpoint: Some(expected),
        publication: None,
    };
    if !publisher.publish_marker(event, undelivered_before, fence_at_walk_start) {
        // `publish_marker` answers false both for a fence refusal at the mint
        // and for a marker that reached no receiver. A fence that no longer
        // equals the walk's is the refusal (the fence only moves forward, so
        // the mint cannot have passed); otherwise the marker reached nobody.
        if bound.coverage().scope_fence(scope) != fence_before {
            tracing::warn!(
                target: "bifrost.sync.backfill",
                scope = ?scope,
                "the scope fence moved before its completion marker could be registered; \
                 withholding the marker"
            );
        } else {
            tracing::warn!(
                target: "bifrost.sync.backfill",
                scope = ?scope,
                "its completion marker reached no consumer; withholding it so the scope \
                 is re-walked"
            );
        }
        return MarkerOutcome::Withheld;
    }

    MarkerOutcome::Published
}

/// What became of a scope's completion marker.
///
/// Three answers rather than two, and the third is the point: an earlier
/// revision returned one `false` for "the account is going away" and for "this
/// walk was invalidated while the marker waited", and the caller could only read
/// it as shutdown - so a single scope reset landing inside one marker's capacity
/// wait retired the whole orchestrator, abandoning every later scope and every
/// later rescan until the account was reattached.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum MarkerOutcome {
    /// Published to a numbered receiver.
    Published,
    /// Deliberately not published: the scope was reset, or a page of this walk
    /// reached nobody, while the marker waited. The scope is left unsettled and
    /// the orchestrator carries on with the next one.
    Withheld,
    /// The account is tearing down.
    ShuttingDown,
}

#[cfg(test)]
mod tests {
    use super::{BackfillKey, Partition};

    #[test]
    fn durable_key_codec_reads_existing_rows() {
        for bytes in [
            "full",
            "time::123",
            "time:-456:",
            "uid:7:9",
            "page:500:1000",
            "complete",
            "complete:v2",
            "future:opaque",
            "page:broken",
        ] {
            let partition = Partition(bytes.as_bytes().to_vec());
            assert_eq!(BackfillKey::decode(&partition).encode(), partition);
        }
        assert!(!BackfillKey::LegacyComplete.proves_completion(1));
        assert!(BackfillKey::LegacyComplete.proves_completion(2));
        assert!(BackfillKey::Complete.proves_completion(0));
    }
}
