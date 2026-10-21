//! Stream event types shared between protocol crates and the engine.
//!
//! Every Account-trait batched stream yields `SyncEvent<T>`, where
//! `SyncEvent::Batch(Batch<T>)` is the dominant variant. The
//! checkpoint travels inside the batch so the consumer persists data
//! and cursor in one transaction. `push_stream` yields raw
//! `WatchEvent`s - those are wake-up signals, not paged data.

use std::time::Duration;

use crate::coverage::{CoverageDomain, CoverageOutcome, InventoryCoverageReport};
use crate::cursor::{ChangeCursor, CursorScope, MembershipScope};
use crate::error::{AccountError, Warning};
use crate::ids::{AccountId, ObjectId};
use crate::mutation::Fingerprint;

/// Cursor and backfill progress checkpoint persisted by the consumer.
///
/// Opaque to the consumer: protocol-owned bytes plus an envelope tag.
/// Carried inside `Batch` at advance boundaries; never travels as its
/// own event. Resuming from a checkpoint whose covering batch was not
/// durably written is unsafe.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Checkpoint {
    Change(ChangeCursor),
    Backfill(BackfillCheckpoint),
}

/// Complete durable checkpoint boundary for one account.
///
/// One account may have several cursor scopes and several concurrently driven
/// backfill partitions. A single [`Checkpoint`] cannot describe that state, so
/// control boundaries return this collection instead. There is at most one
/// entry for each change scope and each `(scope, partition)` backfill lane -
/// [`DurableCheckpointSet::new`] enforces that, so a duplicate lane is not
/// representable and equality cannot depend on which side of the comparison a
/// repeated entry landed on.
#[derive(Debug, Clone, Default)]
pub struct DurableCheckpointSet {
    checkpoints: Vec<Checkpoint>,
}

/// Identity of the durable slot a checkpoint occupies. Two checkpoints with the
/// same lane describe the same durable row, so only the later one is retained.
///
/// `Checkpoint` is `#[non_exhaustive]` to its consumers, but this match is
/// deliberately exhaustive in the defining crate: adding a variant must be a
/// compile error here, so a new checkpoint kind cannot silently acquire another
/// variant's lane and evict its durable entry.
#[derive(PartialEq)]
enum CheckpointLane<'a> {
    Change(&'a CursorScope),
    Backfill(&'a CursorScope, &'a Partition),
}

impl<'a> CheckpointLane<'a> {
    fn of(checkpoint: &'a Checkpoint) -> Self {
        match checkpoint {
            Checkpoint::Change(cursor) => Self::Change(&cursor.scope),
            Checkpoint::Backfill(backfill) => Self::Backfill(&backfill.scope, &backfill.partition),
        }
    }
}

impl PartialEq for DurableCheckpointSet {
    /// Order-insensitive, and symmetric because it compares multiplicities in
    /// both directions rather than one-way containment.
    fn eq(&self, other: &Self) -> bool {
        fn count(haystack: &[Checkpoint], needle: &Checkpoint) -> usize {
            haystack.iter().filter(|entry| *entry == needle).count()
        }
        self.checkpoints.len() == other.checkpoints.len()
            && self
                .checkpoints
                .iter()
                .all(|entry| count(&self.checkpoints, entry) == count(&other.checkpoints, entry))
    }
}

impl Eq for DurableCheckpointSet {}

impl DurableCheckpointSet {
    /// Normalizes to at most one entry per durable lane, keeping the last
    /// occurrence of each - callers build the vector in publication order, so
    /// the last one is the most recently made durable.
    #[must_use]
    pub fn new(checkpoints: Vec<Checkpoint>) -> Self {
        let mut normalized: Vec<Checkpoint> = Vec::with_capacity(checkpoints.len());
        for checkpoint in checkpoints {
            let existing = {
                let lane = CheckpointLane::of(&checkpoint);
                normalized
                    .iter()
                    .position(|candidate| CheckpointLane::of(candidate) == lane)
            };
            match existing {
                Some(slot) => normalized[slot] = checkpoint,
                None => normalized.push(checkpoint),
            }
        }
        Self {
            checkpoints: normalized,
        }
    }

    #[must_use]
    pub fn checkpoints(&self) -> &[Checkpoint] {
        &self.checkpoints
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.checkpoints.is_empty()
    }
}

/// Backfill checkpoint. Partition-aware, finite. The engine's
/// backfill scheduler partitions newest-first so foreground mail is
/// hydrated before deep history.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BackfillCheckpoint {
    pub scope: CursorScope,
    pub partition: Partition,
    pub progress_marker: Option<crate::cursor::OpaqueProgressBytes>,
    pub progress: BackfillProgress,
    pub envelope_version: u32,
}

/// Backfill partition descriptor. Consumer-store key, opaque outside
/// the engine. `InventoryPartition` is the Account-facing shape; this
/// byte key is the durable checkpoint identity derived from it.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Partition(pub Vec<u8>);

/// Account-facing inventory partition.
///
/// `Full` preserves the original one-pass inventory contract. Other
/// variants let the engine request a finite slice without coupling to
/// a specific protocol crate.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum InventoryPartition {
    Full,
    /// Time window as Unix epoch seconds, inclusive-exclusive.
    Time {
        from_unix_seconds: Option<i64>,
        to_unix_seconds: Option<i64>,
    },
    /// UID range, inclusive-exclusive.
    Uid {
        from: u64,
        to: u64,
    },
    /// Count-based page range, inclusive-exclusive.
    Page {
        from: u32,
        to: u32,
    },
}

/// Partitioning modes an Account can honor for a scope.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum InventoryPartitioning {
    /// Only the original full `inventory_stream(scope)` pass is
    /// supported.
    Full,
    /// Account can honor `InventoryPartition::Time` windows.
    TimeWindowed,
    /// Account can honor `InventoryPartition::Uid` ranges. `max_uid`
    /// is optional because some protocol impls only know it after a
    /// folder open; the engine falls back to `Full` when it is absent.
    UidRange { max_uid: Option<u32> },
    /// Account can honor `InventoryPartition::Page` ranges. `total`
    /// is optional; when absent, the engine walks page windows until a
    /// partition's completion declares [`PartitionEnd::Exhausted`]. A short
    /// or even empty window that declares `MoreBeyond` does not end the
    /// walk, and a partition stream that ends without any completion ends
    /// it without completing the scope. A producer built on
    /// [`lift_complete_walk`] cannot declare, and gets `Exhausted` exactly
    /// when its window yielded no entry, so it must keep paging past a
    /// window whose ids all vanished rather than return it empty.
    /// `page_size` lets the account cap engine requests at a
    /// protocol-advertised per-page maximum.
    PageCount {
        total: Option<u32>,
        page_size: Option<u32>,
    },
}

/// Progress within a single backfill partition.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BackfillProgress {
    pub items_done: u64,
    pub items_estimated: Option<u64>,
}

/// Page boundary marker on a batch.
///
/// `kind` is metadata for observability. The cursor advance itself
/// lives on `Batch::checkpoint`, not here - keeping two encodings
/// (one in PageBoundary, one in Batch.checkpoint) let them disagree.
/// `Batch::checkpoint = Some(_)` IS the cursor-advance signal; the
/// page boundary just tells the engine what shape of boundary it is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PageBoundary {
    /// Mid-page partial flush (e.g., max_wait elapsed before page
    /// filled). Batch::checkpoint is `None`.
    Partial,
    /// Natural page boundary as the protocol crate defines it.
    /// Batch::checkpoint MAY be `Some` if the cursor advanced.
    Page,
    /// Stream terminus. Batch::checkpoint carries the final cursor
    /// if there is one to carry.
    Final,
}

/// Streaming batch envelope.
///
/// The consumer atomically persists `(items, checkpoint.unwrap())` in
/// one transaction when `checkpoint` is `Some`.
///
/// The fields stay public and `try_new` is therefore a CONVENIENCE, not a
/// gate: an implementor can still assemble `Partial` + `Some(checkpoint)`
/// literally and never touch the constructor. Making the fields private
/// would enforce it, at the cost of deleting published fields every
/// protocol crate constructs, which is not on the table. So the invariant
/// is enforced where it can actually be enforced - `bifrost-sync` calls
/// `validate_boundary` on every batch it receives from an account and
/// terminates the scope with a classified `ProviderContractViolation`
/// rather than trusting the shape. Producers should use `try_new` to find
/// out at the source; consumers must not assume they did.
#[derive(Debug, Clone)]
pub struct Batch<T> {
    pub items: Vec<T>,
    pub page_boundary: PageBoundary,
    pub server_latency: Duration,
    pub bytes_in: u64,
    pub checkpoint: Option<Checkpoint>,
}

impl<T> Batch<T> {
    pub fn try_new(
        items: Vec<T>,
        page_boundary: PageBoundary,
        server_latency: Duration,
        bytes_in: u64,
        checkpoint: Option<Checkpoint>,
    ) -> Result<Self, BatchBoundaryError> {
        let batch = Self {
            items,
            page_boundary,
            server_latency,
            bytes_in,
            checkpoint,
        };
        batch.validate_boundary()?;
        Ok(batch)
    }

    /// Reject the one boundary/checkpoint combination that cannot describe a
    /// durable transaction. Consumers must call this at the account boundary.
    pub fn validate_boundary(&self) -> Result<(), BatchBoundaryError> {
        if matches!(self.page_boundary, PageBoundary::Partial) && self.checkpoint.is_some() {
            Err(BatchBoundaryError::PartialCarriesCheckpoint)
        } else {
            Ok(())
        }
    }
}

/// A batch or inventory completion whose checkpoint cannot describe a durable
/// transaction.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum BatchBoundaryError {
    /// A `PageBoundary::Partial` batch carried a checkpoint.
    PartialCarriesCheckpoint,
    /// An inventory checkpoint's [`PageCheckpoint`] variant disagrees with its
    /// coverage: `Withheld` without a barrier, or a barrier with no
    /// `Withheld`.
    CheckpointDisagreesWithCoverage,
}

impl BatchBoundaryError {
    /// The violation in words, for the classified contract error.
    #[must_use]
    pub fn detail(&self) -> &'static str {
        match self {
            Self::PartialCarriesCheckpoint => "a PageBoundary::Partial batch carried a checkpoint",
            Self::CheckpointDisagreesWithCoverage => {
                "an inventory checkpoint's withheld state disagreed with its coverage barriers"
            }
        }
    }
}

impl std::fmt::Display for BatchBoundaryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.detail())
    }
}

impl std::error::Error for BatchBoundaryError {}

/// One page of an inventory walk, plus what the walk PROVED up to here.
///
/// Distinct from `Batch<InventoryEntry>` because a page of an enumeration has
/// to say more than a page of anything else: an advancing checkpoint must name
/// every unresolved obligation preceding it, in the same event, or the
/// checkpoint certifies coverage it does not have.
///
/// `coverage` describes THIS batch and everything before it, not just this
/// batch. Obligations ride checkpoint-bearing batches rather than only the
/// terminal completion because Graph and `BackfillRunner` both advance
/// per page - waiting for `Done` would let a page checkpoint become durable
/// across a gap it never declared.
///
/// `checkpoint` is a [`PageCheckpoint`], not an `Option<Checkpoint>`, so a page
/// whose coverage holds a barrier cannot be read as an ordinary advancing page:
/// an `Option` could not tell "no checkpoint" from "a checkpoint the barrier
/// forbids", and a consumer once advanced past a barrier for exactly that
/// reason. The fields are public, so the agreement between the variant and
/// `coverage` is checked by [`Self::validate_boundary`], which `bifrost-sync`
/// runs on every page it receives.
#[derive(Debug, Clone)]
pub struct InventoryBatch {
    pub items: Vec<InventoryEntry>,
    /// Objects removed from this inventory scope. Apply with the entries and
    /// accepted page checkpoint in one consumer transaction. Folder and query
    /// scopes remove membership; account and type scopes remove the object.
    /// Providers without inventory tombstones leave this empty.
    pub removed: Vec<ObjectId>,
    pub page_boundary: PageBoundary,
    pub server_latency: Duration,
    pub bytes_in: u64,
    pub checkpoint: PageCheckpoint,
    pub coverage: InventoryCoverageReport,
}

impl InventoryBatch {
    /// Build a page, deriving the [`PageCheckpoint`] variant from `coverage`.
    /// Initializes `removed` empty; providers with tombstones fill it before
    /// emitting the page.
    pub fn try_new(
        items: Vec<InventoryEntry>,
        page_boundary: PageBoundary,
        server_latency: Duration,
        bytes_in: u64,
        checkpoint: Option<Checkpoint>,
        coverage: InventoryCoverageReport,
    ) -> Result<Self, BatchBoundaryError> {
        let batch = Self {
            items,
            removed: Vec::new(),
            page_boundary,
            server_latency,
            bytes_in,
            checkpoint: PageCheckpoint::new(checkpoint, &coverage),
            coverage,
        };
        batch.validate_boundary()?;
        Ok(batch)
    }

    /// Reject a partial page carrying a checkpoint, and a checkpoint whose
    /// variant disagrees with `coverage`. Consumers must call this at the
    /// account boundary.
    pub fn validate_boundary(&self) -> Result<(), BatchBoundaryError> {
        if matches!(self.page_boundary, PageBoundary::Partial)
            && self.checkpoint.offered().is_some()
        {
            Err(BatchBoundaryError::PartialCarriesCheckpoint)
        } else if !self.checkpoint.agrees_with(&self.coverage) {
            Err(BatchBoundaryError::CheckpointDisagreesWithCoverage)
        } else {
            Ok(())
        }
    }
}

/// The position an inventory page or completion offers, and whether the walk's
/// coverage lets it be taken.
///
/// The variant is a function of the coverage it travels with: `Withheld`
/// exactly when that coverage holds a checkpoint barrier
/// ([`InventoryCoverageReport::has_barrier`]). [`Self::new`] derives it, and
/// the batch and completion validators reject a literal that disagrees. A
/// barrier taints the walk, so a cumulative coverage report keeps every page
/// after it `Withheld` too.
///
/// The one way to take a position is [`Self::accepted`], which makes the
/// caller say whether the barriers were waived. [`Self::offered`] ignores the
/// barrier and exists for diagnostics and the partial-boundary check only.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PageCheckpoint {
    /// The page certifies no position.
    None,
    /// No barrier stands in the walk's coverage, so the position may be taken.
    Advance(Checkpoint),
    /// The walk's coverage holds a checkpoint barrier, so no position may be
    /// taken unless an operator waived every barrier in it. Carries the
    /// position the producer offered, if any, for that waived crossing.
    Withheld(Option<Checkpoint>),
}

impl PageCheckpoint {
    /// The variant `coverage` calls for, around the position the producer
    /// offers.
    #[must_use]
    pub fn new(checkpoint: Option<Checkpoint>, coverage: &InventoryCoverageReport) -> Self {
        if coverage.has_barrier() {
            Self::Withheld(checkpoint)
        } else {
            checkpoint.map_or(Self::None, Self::Advance)
        }
    }

    /// The position that may be taken: an `Advance` one, or a `Withheld` one
    /// when `barriers_waived` says an operator waived every barrier.
    #[must_use]
    pub fn accepted(&self, barriers_waived: bool) -> Option<&Checkpoint> {
        match self {
            Self::None => None,
            Self::Advance(checkpoint) => Some(checkpoint),
            Self::Withheld(checkpoint) => checkpoint.as_ref().filter(|_| barriers_waived),
        }
    }

    /// The position the producer offered, whatever the barrier says. Never an
    /// advancement decision.
    #[must_use]
    pub fn offered(&self) -> Option<&Checkpoint> {
        match self {
            Self::None => None,
            Self::Advance(checkpoint) => Some(checkpoint),
            Self::Withheld(checkpoint) => checkpoint.as_ref(),
        }
    }

    #[must_use]
    pub fn is_withheld(&self) -> bool {
        matches!(self, Self::Withheld(_))
    }

    /// Whether this variant is the one `coverage` calls for.
    #[must_use]
    pub fn agrees_with(&self, coverage: &InventoryCoverageReport) -> bool {
        self.is_withheld() == coverage.has_barrier()
    }
}

/// Whether results exist past the end of an inventory partition, as the
/// producer DECLARES it.
///
/// An open-ended page walk stops on this declaration. It used to infer the end
/// from a page that yielded zero entries, which made every producer owe a
/// stronger contract than the type showed (fill the window, page past any
/// server cap, never yield an empty window early), and a short-page-means-done
/// inference crept back into the resume path once. A declaration removes that
/// class: the producer, which is the only party that knows its listing ran dry,
/// says so.
///
/// A whole-scope walk that finished is `Exhausted` by definition. For a fixed
/// partition set (time windows, UID ranges) the engine walks every partition
/// regardless, so the value is informational there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PartitionEnd {
    /// The enumeration has no results past this partition's end.
    Exhausted,
    /// Results may exist past this partition's end.
    MoreBeyond,
}

/// How an inventory walk ended.
///
/// Deliberately NOT expressed as `Done(None)`. That already means "this stream
/// established no cursor" for discovery and no-op passes, maps to
/// `FusionOutcome::NoCursor`, and loses the reason - which invites a caller to
/// read end-of-stream as success. An incomplete walk is a different fact from
/// an empty one and gets its own carrier.
#[derive(Debug, Clone)]
pub struct InventoryCompletion {
    /// The walk's final position, gated by `coverage` exactly as a page's is.
    pub checkpoint: PageCheckpoint,
    /// Coverage for the walk as a whole. `Degraded` means the enumeration
    /// space was NOT exhausted cleanly, so a consumer must not read `Done` as
    /// proof of completeness and the engine must not write a backfill
    /// completion sentinel.
    pub coverage: InventoryCoverageReport,
    /// Whether the enumeration continues past this partition.
    pub end: PartitionEnd,
}

impl InventoryCompletion {
    /// A completion, deriving the [`PageCheckpoint`] variant from `coverage`.
    #[must_use]
    pub fn new(
        checkpoint: Option<Checkpoint>,
        coverage: InventoryCoverageReport,
        end: PartitionEnd,
    ) -> Self {
        Self {
            checkpoint: PageCheckpoint::new(checkpoint, &coverage),
            coverage,
            end,
        }
    }

    /// A walk that exhausted `domain` with nothing left unaccounted for.
    ///
    /// Takes the domain because a completeness claim is only meaningful about a
    /// stated extent: "complete" is a fact about a region of one enumeration,
    /// not about a scope in the abstract, and a claim with no domain cannot be
    /// checked against the debt it would discharge. `end` says whether the
    /// enumeration continues past the domain; a whole-scope walk is
    /// `Exhausted`.
    #[must_use]
    pub fn complete(
        domain: CoverageDomain,
        checkpoint: Option<Checkpoint>,
        end: PartitionEnd,
    ) -> Self {
        Self::new(checkpoint, InventoryCoverageReport::complete(domain), end)
    }

    /// Reject a checkpoint whose variant disagrees with `coverage`. Consumers
    /// must call this at the account boundary, as they do for each page.
    pub fn validate(&self) -> Result<(), BatchBoundaryError> {
        if self.checkpoint.agrees_with(&self.coverage) {
            Ok(())
        } else {
            Err(BatchBoundaryError::CheckpointDisagreesWithCoverage)
        }
    }
}

#[cfg(test)]
mod inventory_completion_tests {
    use super::*;
    use crate::coverage::{InventoryObligation, ObligationKey, RegionRecovery};
    use crate::error::{
        AccountErrorBuilder, AccountErrorKind, Cause, DiagnosticText, RequestCause,
        RequestErrorKind,
    };

    fn checkpoint() -> Checkpoint {
        Checkpoint::Backfill(BackfillCheckpoint {
            scope: CursorScope::Account,
            partition: Partition(b"page:0:10".to_vec()),
            progress_marker: None,
            progress: BackfillProgress::default(),
            envelope_version: 1,
        })
    }

    fn with_barrier() -> InventoryCoverageReport {
        let error = AccountErrorBuilder::new(
            AccountErrorKind::Request(RequestErrorKind::Malformed),
            Cause::Request(RequestCause::Malformed {
                detail: DiagnosticText::support_only("barrier"),
            }),
        )
        .try_build()
        .expect("valid error");
        InventoryCoverageReport::degraded(
            CoverageDomain::full(CursorScope::Account),
            vec![InventoryObligation::Region {
                key: ObligationKey(b"page".to_vec()),
                failure_label: "unidentifiable".into(),
                error,
                recovery: RegionRecovery::barrier(),
            }],
        )
    }

    #[test]
    fn complete_preserves_the_exact_partition_domain() {
        let domain = CoverageDomain::for_partition(
            CursorScope::Account,
            &InventoryPartition::Page { from: 10, to: 20 },
            0,
        );
        let completion =
            InventoryCompletion::complete(domain.clone(), None, PartitionEnd::MoreBeyond);
        assert_eq!(completion.coverage.domain, domain);
        assert!(completion.coverage.is_complete());
        assert_eq!(completion.end, PartitionEnd::MoreBeyond);
    }

    /// A barrier withholds the position: it is not taken unless the barriers
    /// were waived, and the producer's offer survives for that crossing.
    #[test]
    fn a_barrier_withholds_the_checkpoint_until_waived() {
        let page = PageCheckpoint::new(Some(checkpoint()), &with_barrier());
        assert_eq!(page, PageCheckpoint::Withheld(Some(checkpoint())));
        assert_eq!(page.accepted(false), None);
        assert_eq!(page.accepted(true), Some(&checkpoint()));
        assert_eq!(page.offered(), Some(&checkpoint()));

        let clean = InventoryCoverageReport::complete(CoverageDomain::full(CursorScope::Account));
        let page = PageCheckpoint::new(Some(checkpoint()), &clean);
        assert_eq!(page.accepted(false), Some(&checkpoint()));
        assert_eq!(PageCheckpoint::new(None, &clean), PageCheckpoint::None);
    }

    /// The public fields let a producer write a variant its coverage does not
    /// call for. Both directions are refused at validation.
    #[test]
    fn a_checkpoint_that_disagrees_with_its_coverage_is_refused() {
        let clean = InventoryCoverageReport::complete(CoverageDomain::full(CursorScope::Account));
        let advance_over_barrier = InventoryBatch {
            removed: Vec::new(),
            items: Vec::new(),
            page_boundary: PageBoundary::Page,
            server_latency: Duration::ZERO,
            bytes_in: 0,
            checkpoint: PageCheckpoint::Advance(checkpoint()),
            coverage: with_barrier(),
        };
        assert_eq!(
            advance_over_barrier.validate_boundary(),
            Err(BatchBoundaryError::CheckpointDisagreesWithCoverage)
        );
        let withheld_without_barrier = InventoryCompletion {
            checkpoint: PageCheckpoint::Withheld(None),
            coverage: clean.clone(),
            end: PartitionEnd::Exhausted,
        };
        assert_eq!(
            withheld_without_barrier.validate(),
            Err(BatchBoundaryError::CheckpointDisagreesWithCoverage)
        );
        assert!(
            InventoryBatch::try_new(
                Vec::new(),
                PageBoundary::Page,
                Duration::ZERO,
                0,
                Some(checkpoint()),
                with_barrier(),
            )
            .is_ok(),
            "the constructor derives the variant, so it always agrees"
        );
        assert_eq!(
            InventoryBatch::try_new(
                Vec::new(),
                PageBoundary::Partial,
                Duration::ZERO,
                0,
                Some(checkpoint()),
                clean,
            )
            .err(),
            Some(BatchBoundaryError::PartialCarriesCheckpoint)
        );
    }
}

/// Inventory's own stream envelope.
///
/// Inventory does not use `SyncEvent<T>`. The generic envelope is shared by
/// hydration, changes, discovery, blobs and mutation, and coverage is
/// meaningless to all of them - adding an arm or a field there would force an
/// irrelevant lane onto every consumer of every stream, which is precisely the
/// "stale consumer policy silently applies to a new lane" hazard `ItemOutcome`
/// is documented against.
///
/// It equally cannot live in the element type alone: an element can report an
/// obligation, but only the envelope can say whether a later checkpoint
/// includes that obligation atomically, or whether the stream finished with
/// coverage unproven.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum InventoryEvent {
    Batch(InventoryBatch),
    Progress(Progress),
    Warning(Warning),
    /// The walk cannot continue at all. Distinct from finishing with degraded
    /// coverage: this is a whole-response failure, not an item or region one.
    Terminated(AccountError),
    Done(InventoryCompletion),
}

/// Lift a generic stream event into the inventory envelope, asserting COMPLETE
/// coverage over `domain`.
///
/// The domain is the caller's to state, and a PARTITION walk must not pass a
/// full-scope domain: a partition that finished cleanly proves nothing about
/// the partitions either side of it, and a `Full` claim would discharge their
/// debt on the strength of an unrelated range's success.
///
/// For producers that have no way to express an unresolved obligation: their
/// behaviour is to terminate the whole walk on any failure, which never
/// advances a checkpoint across a gap, so claiming `Complete` is accurate for
/// them. A producer that starts absorbing failures and continuing MUST stop
/// using this and build `InventoryBatch` / `InventoryCompletion` itself -
/// otherwise it reports full coverage over a walk that skipped something, which
/// is the exact failure this envelope exists to make impossible.
///
/// Deliberately a scope-taking adapter rather than a `From` impl. A bare
/// `.into()` manufactured a completeness claim out of nothing, with no extent
/// attached and no syntax at the call site to notice - the same shape as every
/// other implicit-`Complete` path that let a durable record certify coverage it
/// did not have.
///
/// The [`PartitionEnd`] a `SyncEvent` stream cannot declare is the one thing
/// this adapter infers, and this is the only place in the workspace that does.
/// A whole-scope domain is `Exhausted` by definition. A partition domain is
/// `Exhausted` exactly when the stream yielded no entry at all, the contract
/// open-ended page walks used to rely on everywhere: a lifted page stream must
/// yield zero entries only when the scope has nothing past the window. A
/// producer that can say where its listing ran dry should build
/// `InventoryCompletion` itself and declare it.
pub fn lift_complete_walk(
    domain: CoverageDomain,
) -> impl FnMut(SyncEvent<InventoryEntry>) -> InventoryEvent {
    let whole_scope = matches!(domain.coordinate, crate::coverage::CoverageCoordinate::Full);
    let report = move || InventoryCoverageReport {
        domain: domain.clone(),
        outcome: CoverageOutcome::Complete,
    };
    let mut seen_any = false;
    move |event| match event {
        SyncEvent::Batch(batch) => {
            seen_any |= !batch.items.is_empty();
            InventoryEvent::Batch(InventoryBatch {
                removed: Vec::new(),
                items: batch.items,
                page_boundary: batch.page_boundary,
                server_latency: batch.server_latency,
                bytes_in: batch.bytes_in,
                checkpoint: batch
                    .checkpoint
                    .map_or(PageCheckpoint::None, PageCheckpoint::Advance),
                coverage: report(),
            })
        }
        SyncEvent::Progress(progress) => InventoryEvent::Progress(progress),
        SyncEvent::Warning(warning) => InventoryEvent::Warning(warning),
        SyncEvent::Terminated(error) => InventoryEvent::Terminated(error),
        SyncEvent::Done(checkpoint) => InventoryEvent::Done(InventoryCompletion::new(
            checkpoint,
            report(),
            if whole_scope || !seen_any {
                PartitionEnd::Exhausted
            } else {
                PartitionEnd::MoreBeyond
            },
        )),
    }
}

/// Top-level event from every Account-trait stream.
///
/// `Done(Option<Checkpoint>)` carries the final checkpoint at stream
/// completion. `None` for streams that terminate before establishing
/// or advancing any cursor (a discovery stream, a no-op pass).
///
/// The `Batch` variant dominates traffic (one per page, every page);
/// `Progress` / `Warning` / `Terminated` / `Done` are rare. Boxing
/// the rare arms would slow the hot path without saving real memory,
/// so we accept the size asymmetry instead.
///
/// `Terminated(AccountError)` signals that this stream ends here. The
/// engine reads `error.recovery()` to decide what to do next. It is
/// distinct from `bifrost-types::error::Fatal`, which collapses any
/// terminal `RecoveryClass` at the engine boundary; stream termination
/// is not always a terminal error (the engine may retry or restart).
#[derive(Debug)]
#[non_exhaustive]
#[allow(clippy::large_enum_variant)]
pub enum SyncEvent<T> {
    Batch(Batch<T>),
    Progress(Progress),
    Warning(Warning),
    Terminated(AccountError),
    Done(Option<Checkpoint>),
}

/// Coarse, consumer-visible progress for long streams.
#[derive(Debug, Clone, Copy, Default)]
pub struct Progress {
    pub items_done: u64,
    pub bytes_in: u64,
    pub estimated_total: Option<u64>,
    pub eta: Option<Duration>,
}

/// Object change emitted by `changes_stream`.
///
/// No `memberships` field: scope changes are a separate variant.
#[derive(Debug, Clone)]
pub struct ObjectChange {
    pub id: ObjectId,
    pub kind: ObjectChangeKind,
}

/// Per-object change kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ObjectChangeKind {
    /// Optional precision hint. Consumers must also tolerate an existing id.
    Created,
    /// Upsert: refresh an existing object or create one for an unknown id.
    Updated,
    Destroyed,
}

/// Object-to-container membership change.
///
/// `membership` stays a single `MembershipScope`, not a `Vec`
/// (adjudicated, likely WONTFIX): the single `Folder(id)` is the
/// correct routing membership - the engine's covering rule would not
/// cover a `Mailbox(owner)` tag against a folder cursor. Widening
/// would touch types + jmap + graph + engine and is only worth it if
/// change events must carry the owner tag for some future consumer.
#[derive(Debug, Clone)]
pub struct ScopeChange {
    pub id: ObjectId,
    pub membership: MembershipScope,
    pub kind: ScopeChangeKind,
}

/// Per-scope change kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum ScopeChangeKind {
    Added,
    Removed,
}

/// Sum type yielded by `changes_stream`. Consumers that care only
/// about object state filter to `ObjectChange`.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum Change {
    ObjectChange(ObjectChange),
    ScopeChange(ScopeChange),
}

/// Inventory entry yielded by `inventory_stream`.
///
/// Projection-only cold-start primitive. `memberships` is a `Vec`
/// because Gmail messages routinely sit in many labels and JMAP
/// messages can sit in many mailboxes.
#[derive(Debug, Clone)]
pub struct InventoryEntry {
    pub id: ObjectId,
    pub memberships: Vec<MembershipScope>,
    /// Message size in bytes. `Option` because Microsoft Graph does
    /// not expose `size` on the message resource; consumers fall
    /// back to `(server_version, flags_hash)` for diff there.
    pub size: Option<u64>,
    pub blob_id: Option<crate::ids::BlobId>,
    pub fingerprint: Fingerprint,
    pub thread_id: Option<crate::ids::ThreadId>,
    pub message_id: Option<String>,
    pub references: Vec<String>,
    pub in_reply_to: Option<String>,
}

impl InventoryEntry {
    /// Whether any inventory-visible representation changed.
    ///
    /// Consumers must compare the complete entry, not only `fingerprint`:
    /// memberships and threading/header projections can change without a
    /// provider changing flags, size, or its version token.
    #[must_use]
    pub fn differs_from(&self, other: &Self) -> bool {
        self.id != other.id
            || self.memberships != other.memberships
            || self.size != other.size
            || self.blob_id != other.blob_id
            || self.fingerprint != other.fingerprint
            || self.thread_id != other.thread_id
            || self.message_id != other.message_id
            || self.references != other.references
            || self.in_reply_to != other.in_reply_to
    }
}

/// Push wake-up event. Push surfaces are wake-ups, not change feeds.
///
/// `Terminated(AccountError)` carries a classified error when the push
/// stream ends and cannot be reconnected without engine intervention
/// (auth lost, subscription deleted, schema break). It is the push
/// equivalent of [`SyncEvent::Terminated`]. `Disconnected` /
/// `Reconnected` remain advisory: a transient transport drop emits
/// `Disconnected` followed by `Reconnected` once the renewer succeeds.
///
/// `Warning(Warning)` carries a structured advisory event (renewal
/// hiccups, throttle telemetry, etc.) without ending the stream. Push
/// transports that classify a transient error use this rather than
/// bare-`tracing::warn!` so consumers and the engine see a typed
/// signal.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum WatchEvent {
    Invalidated { hint: InvalidationHint },
    Disconnected,
    Reconnected,
    Terminated(AccountError),
    Warning(Warning),
}

/// Push payload, type-erased to a protocol-agnostic shape.
#[derive(Debug, Clone)]
pub struct InvalidationHint {
    pub source: PushSource,
    pub payload: HintPayload,
}

/// Which push transport originated the wake-up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PushSource {
    JmapStateChange,
    ImapNotify,
    GmailPubsub,
    GraphSubscription,
    EwsStreaming,
    /// Engine-synthesized: the sink overflowed and the precise source
    /// could not be preserved; the wake-up is a coalesced full
    /// reconcile.
    Coalesced,
}

/// Hint about which scope was touched. Engine treats `Unknown` and a
/// specific hint identically in v1.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum HintPayload {
    SpecificCursorScope(CursorScope),
    SpecificMembership(MembershipScope),
    Unknown,
}

/// Sink for out-of-process push events (Gmail Pub/Sub listeners,
/// Graph webhook receivers). The consumer wires its receiver to
/// invoke `push` on every event; the engine merges sink-injected
/// events with in-process events into one logical push channel per
/// account.
pub trait InvalidationSink: Send + Sync + 'static {
    fn push(&self, account: AccountId, event: WatchEvent);
}

/// Consumer-to-producer control handle returned alongside every
/// engine-driven stream.
///
/// `pause` and `checkpoint_now` are async because they must wait for
/// "stream is at a safe boundary, here is the latest checkpoint if
/// one exists, you can now drop." The returned set is empty when an idle
/// account has never produced a checkpoint. `resume`, `priority`,
/// `bandwidth_cap`, and
/// `bandwidth_observed` stay synchronous (fire-and-forget signals or
/// pure reads).
pub trait Control: Send + Sync {
    fn pause(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<DurableCheckpointSet, AccountError>>
                + Send
                + '_,
        >,
    >;
    fn checkpoint_now(
        &self,
    ) -> std::pin::Pin<
        Box<
            dyn std::future::Future<Output = Result<DurableCheckpointSet, AccountError>>
                + Send
                + '_,
        >,
    >;
    fn resume(&self);
    fn priority(&self, p: Priority);
    fn bandwidth_cap(&self, bps: Option<u64>);
    fn bandwidth_observed(&self) -> u64;
}

/// Scheduling priority on the four-lane scheduler.
///
/// `#[repr(u8)]` so the discriminant is stable for `AtomicU8`
/// storage in hot per-request paths (see `bifrost-net`'s
/// `AccountNet`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
#[non_exhaustive]
pub enum Priority {
    /// User-visible; preempts background work.
    Foreground = 0,
    /// Default.
    Normal = 1,
    /// Backfill, archive-folder polling.
    Background = 2,
    /// Batch operations the user will not watch.
    Bulk = 3,
}

/// Per-account control signal exchanged between the engine and the
/// consumer. Carried on `AccountControlEvent` channels so the consumer
/// can pause an account (e.g. on operator-override directives) or
/// resume it after intervention.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum AccountControl {
    Pause(PauseReason),
    Resume,
}

/// Reason an account is paused. Bounded enum, not a free-form string
/// or `DiagnosticText`: the engine is the producer and the set of
/// pause causes is enumerable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum PauseReason {
    /// The protocol or engine surfaced `EngineDirective::
    /// OperatorOverrideRequired`. Consumer must resolve the underlying
    /// issue (typically reachable through the account stream's prior
    /// `Warning::OperatorAttentionNeeded`) before resuming.
    OperatorOverrideRequired,
    /// Consumer-initiated pause via `Control::pause` or equivalent.
    ConsumerRequested,
    /// Engine paused this account because a tenant-level throttle
    /// covers it. Resumes automatically once the throttle clears.
    TenantThrottle,
    /// Engine exhausted its retry budget for a recurring failure;
    /// consumer intervention required before further attempts.
    RetryBudgetExhausted,
}

#[cfg(test)]
mod tests {
    use super::{Batch, Checkpoint, InventoryEntry, PageBoundary};
    use crate::{
        ChangeCursor, CursorScope, Fingerprint, MembershipScope, ObjectId, OpaqueChangeState,
        ProtocolKind, ServerVersion,
    };
    use std::time::Duration;

    fn cursor(scope: CursorScope, bytes: &[u8]) -> Checkpoint {
        Checkpoint::Change(ChangeCursor {
            scope,
            server_state: OpaqueChangeState {
                protocol: ProtocolKind::Imap,
                envelope_version: 1,
                bytes: bytes.to_vec(),
            },
            advanced_through: None,
            envelope_version: 1,
        })
    }

    /// A set built from a repeated lane must not compare equal to one built
    /// from two distinct lanes, in EITHER order.
    ///
    /// The first implementation compared equal lengths and then checked one-way
    /// containment, which made `[a, a] == [a, b]` while `[a, b] != [a, a]` - an
    /// asymmetric `PartialEq`, which is undefined behaviour as far as every
    /// generic container that relies on the contract is concerned.
    #[test]
    fn a_repeated_lane_is_never_equal_to_two_distinct_lanes() {
        let a = cursor(CursorScope::Account, b"a");
        let b = cursor(CursorScope::Type(crate::ObjectType::Email), b"b");

        let repeated = super::DurableCheckpointSet::new(vec![a.clone(), a.clone()]);
        let distinct = super::DurableCheckpointSet::new(vec![a.clone(), b]);

        assert_ne!(repeated, distinct);
        assert_ne!(distinct, repeated);
        assert_eq!(
            repeated,
            super::DurableCheckpointSet::new(vec![a]),
            "a repeated lane normalizes to a single entry"
        );
    }

    /// Order is not part of the value: the same lanes in either order are the
    /// same durable boundary.
    #[test]
    fn lane_order_does_not_change_the_boundary() {
        let a = cursor(CursorScope::Account, b"a");
        let b = cursor(CursorScope::Type(crate::ObjectType::Email), b"b");
        assert_eq!(
            super::DurableCheckpointSet::new(vec![a.clone(), b.clone()]),
            super::DurableCheckpointSet::new(vec![b, a])
        );
    }

    /// The later entry for a lane wins: callers build the vector in publication
    /// order, so keeping the earlier one would report a superseded checkpoint as
    /// the durable boundary.
    #[test]
    fn the_last_entry_for_a_lane_is_the_one_retained() {
        let older = cursor(CursorScope::Account, b"older");
        let newer = cursor(CursorScope::Account, b"newer");
        let set = super::DurableCheckpointSet::new(vec![older, newer.clone()]);
        assert_eq!(set.checkpoints(), &[newer]);
    }

    #[test]
    fn partial_batch_rejects_checkpoint() {
        let batch = Batch::<()> {
            items: vec![],
            page_boundary: PageBoundary::Partial,
            server_latency: Duration::ZERO,
            bytes_in: 0,
            checkpoint: Some(Checkpoint::Change(ChangeCursor {
                scope: CursorScope::Account,
                server_state: OpaqueChangeState {
                    protocol: ProtocolKind::Imap,
                    envelope_version: 1,
                    bytes: vec![1],
                },
                advanced_through: None,
                envelope_version: 1,
            })),
        };
        assert!(batch.validate_boundary().is_err());
    }

    #[test]
    fn inventory_change_comparison_includes_memberships_outside_fingerprint() {
        let first = InventoryEntry {
            id: ObjectId("message".into()),
            memberships: vec![MembershipScope::Mailbox("inbox".into())],
            size: Some(10),
            blob_id: None,
            fingerprint: Fingerprint {
                server_version: ServerVersion::StateAt("same".into()),
                size: Some(10),
                flags_hash: 1,
            },
            thread_id: None,
            message_id: None,
            references: Vec::new(),
            in_reply_to: None,
        };
        let mut moved = first.clone();
        moved.memberships = vec![MembershipScope::Mailbox("archive".into())];

        assert!(first.differs_from(&moved));
    }
}
