//! Backfill partitioner.
//!
//! Plans newest-first partitions for a backfill pass. Three strategies:
//! - `TimeWindowed` (default): exponentially-widening (7d, 30d, 90d,
//!   180d, 365d, then yearly).
//! - `UidRange` (IMAP-on-Basic fallback): walks UID 1:N in fixed-size
//!   chunks.
//! - `PageCount` (JMAP optimization): walks a page count instead of a
//!   time / UID dimension.
//!
//! The planner is pure; the runner consumes a `PartitionPlan`.

use std::time::Duration;

use bifrost_types::{InventoryPartition, Partition};
use jiff::{SignedDuration, Timestamp};

/// Account-level backfill policy.
#[derive(Debug, Clone)]
pub struct BackfillPolicy {
    pub strategy: BackfillStrategy,
    /// Server-vs-client clock skew applied to time-windowed bounds.
    /// Positive means the server is ahead of the client.
    pub clock_skew: Duration,
}

impl Default for BackfillPolicy {
    fn default() -> Self {
        Self {
            strategy: BackfillStrategy::TimeWindowed {
                boundaries: default_time_boundaries(),
            },
            clock_skew: Duration::ZERO,
        }
    }
}

/// Partitioning strategy chosen per account.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub enum BackfillStrategy {
    /// Exponentially-widening time partitions.
    TimeWindowed { boundaries: Vec<Duration> },
    /// IMAP-on-Basic fallback. Walks UID 1:N in chunks.
    UidRange { chunk_size: u32, uid_validity: u32 },
    /// JMAP `queryChanges` page count. Walks a count over the result
    /// set rather than a time/UID dimension.
    PageCount { items_per_partition: u32 },
}

/// Default time-windowed boundaries from `bifrost-sync.md`:
/// `[7d, 30d, 90d, 180d, 365d, 2y, 3y, 4y, 5y]`. Anything older than
/// the last boundary falls into a final open partition.
#[must_use]
pub fn default_time_boundaries() -> Vec<Duration> {
    const DAY: u64 = 24 * 60 * 60;
    vec![
        Duration::from_secs(7 * DAY),
        Duration::from_secs(30 * DAY),
        Duration::from_secs(90 * DAY),
        Duration::from_secs(180 * DAY),
        Duration::from_secs(365 * DAY),
        Duration::from_secs(2 * 365 * DAY),
        Duration::from_secs(3 * 365 * DAY),
        Duration::from_secs(4 * 365 * DAY),
        Duration::from_secs(5 * 365 * DAY),
    ]
}

/// A concrete partition plan. The runner walks `partitions` newest-first.
#[derive(Debug, Clone)]
pub struct PartitionPlan {
    pub partitions: Vec<PartitionBounds>,
}

/// One partition's bounds. Three variants matching `BackfillStrategy`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum PartitionBounds {
    /// `[from, to)` inclusive-exclusive time window.
    Time { from: Timestamp, to: Timestamp },
    /// `[from, to)` inclusive-exclusive UID range.
    Uid {
        uid_validity: u32,
        from: u64,
        to: u64,
    },
    /// `[from, to)` page-count range.
    Page { from: u32, to: u32 },
}

/// Convert planner bounds into the protocol-neutral Account trait
/// partition shape.
#[must_use]
pub fn inventory_partition_for(bounds: &PartitionBounds) -> InventoryPartition {
    match bounds {
        PartitionBounds::Time { from, to } => InventoryPartition::Time(
            bifrost_types::TimeWindow::new(
                if *from == Timestamp::MIN {
                    None
                } else {
                    Some(from.as_second())
                },
                Some(to.as_second()),
            )
            .expect("ordered window"),
        ),
        PartitionBounds::Uid {
            uid_validity,
            from,
            to,
        } => InventoryPartition::Uid(
            bifrost_types::UidWindow::new(*uid_validity, *from, *to).expect("ordered window"),
        ),
        PartitionBounds::Page { from, to } => InventoryPartition::Page(
            bifrost_types::PageWindow::new(*from, *to).expect("ordered window"),
        ),
    }
}

/// Engine-owned durable backfill key. The codec preserves existing store bytes,
/// including unknown keys, so upgrading never moves a consumer's resume row.
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
            "full" => return Self::Full,
            "complete" => return Self::LegacyComplete,
            "complete:v2" => return Self::Complete,
            _ => {}
        }
        if let Some(rest) = text.strip_prefix("uid:") {
            let parts: Vec<_> = rest.split(':').collect();
            if let [epoch, from, to] = parts.as_slice()
                && let (Ok(uid_validity), Ok(from), Ok(to)) =
                    (epoch.parse(), from.parse(), to.parse())
                && bifrost_types::UidWindow::new(uid_validity, from, to).is_some()
            {
                return Self::Uid {
                    uid_validity,
                    from,
                    to,
                };
            }
            // Legacy keys carry no epoch and cannot safely resume a UID window.
            return Self::Opaque(partition.clone());
        }
        if let Some((kind, rest)) = text.split_once(':')
            && let Some((from, to)) = rest.split_once(':')
        {
            match kind {
                "page" => {
                    if let (Ok(from), Ok(to)) = (from.parse(), to.parse())
                        && bifrost_types::PageWindow::new(from, to).is_some()
                    {
                        return Self::Page { from, to };
                    }
                }
                "time" => {
                    let parse = |s: &str| {
                        if s.is_empty() {
                            Some(None)
                        } else {
                            s.parse::<i64>().ok().map(Some)
                        }
                    };
                    if let (Some(from), Some(to)) = (parse(from), parse(to))
                        && bifrost_types::TimeWindow::new(from, to).is_some()
                    {
                        return Self::Time { from, to };
                    }
                }
                _ => {}
            }
        }
        Self::Opaque(partition.clone())
    }

    #[must_use]
    pub fn encode(&self) -> Partition {
        let text = match self {
            Self::Full => "full".to_owned(),
            Self::Time { from, to } => {
                format!("time:{}:{}", optional_i64(*from), optional_i64(*to))
            }
            Self::Uid {
                uid_validity,
                from,
                to,
            } => format!("uid:{uid_validity}:{from}:{to}"),
            Self::Page { from, to } => format!("page:{from}:{to}"),
            Self::Complete => "complete:v2".to_owned(),
            Self::LegacyComplete => "complete".to_owned(),
            Self::Opaque(partition) => return partition.clone(),
        };
        Partition(text.into_bytes())
    }

    /// Legacy empty-plan markers require one inventory recheck. Counts on new
    /// markers are ordinary counts, never an ordering discriminator.
    #[must_use]
    pub fn proves_completion(&self, items_done: u64) -> bool {
        matches!(self, Self::Complete) || matches!(self, Self::LegacyComplete) && items_done > 1
    }
}

/// Stable durable checkpoint key for an inventory partition.
#[must_use]
pub fn partition_key(partition: &InventoryPartition) -> Partition {
    partition.durable_key()
}

fn optional_i64(value: Option<i64>) -> String {
    value.map(|v| v.to_string()).unwrap_or_default()
}

/// Parse a durable `Page` partition key back into its `[from, to)`
/// bounds. The inverse of `partition_key` for
/// `InventoryPartition::Page`; returns `None` for any other partition
/// kind or a malformed key. The backfill orchestrator uses it to resume
/// an open-ended page walk from the furthest durably-checkpointed window
/// rather than re-walking from page 0 on every re-attach.
#[must_use]
pub fn parse_page_partition(partition: &Partition) -> Option<(u32, u32)> {
    match BackfillKey::decode(partition) {
        BackfillKey::Page { from, to } => Some((from, to)),
        _ => None,
    }
}

/// Sentinel partition key marking a scope's backfill as fully exhausted.
/// It does not name a window (no `from:to`); it is a durable "backfill is
/// done" flag the orchestrator writes via the normal consumer-ack path on
/// completion, for both fixed plans and open-ended page walks. Distinct
/// from every real `page:F:T` / `uid:E:F:T` / `time:F:T` key so it never
/// collides with a partition checkpoint. Stores select it by row kind,
/// independently of the number of entries observed during this walk.
#[must_use]
pub fn completion_partition() -> Partition {
    // The unversioned marker could be earned by an empty plan without any
    // inventory. Its bytes cannot distinguish that from a legitimate walk.
    // A new version distinguishes a genuinely empty inventory from that old
    // marker; resume rechecks legacy zero/one markers once for missing history.
    BackfillKey::Complete.encode()
}

/// True if `partition` names a completion sentinel, including the legacy key.
/// Resume separately checks whether a legacy marker proves any inventory.
#[must_use]
pub fn is_completion_partition(partition: &Partition) -> bool {
    matches!(
        BackfillKey::decode(partition),
        BackfillKey::Complete | BackfillKey::LegacyComplete
    )
}

/// Plan a partition list for the given policy.
///
/// `now` is taken as a parameter so tests are deterministic. The
/// `total_for_uid_or_page` parameter is the upper bound for `UidRange`
/// (total UIDs in folder) and `PageCount` (total items in query).
#[must_use]
pub fn plan(policy: &BackfillPolicy, now: Timestamp, total_for_uid_or_page: u32) -> PartitionPlan {
    match &policy.strategy {
        BackfillStrategy::TimeWindowed { boundaries } => {
            plan_time(boundaries, now, policy.clock_skew)
        }
        BackfillStrategy::UidRange {
            chunk_size,
            uid_validity,
        } => plan_uid(*chunk_size, total_for_uid_or_page, *uid_validity),
        BackfillStrategy::PageCount {
            items_per_partition,
        } => plan_page(*items_per_partition, total_for_uid_or_page),
    }
}

fn plan_time(boundaries: &[Duration], now: Timestamp, skew: Duration) -> PartitionPlan {
    let now_skewed = SignedDuration::try_from(skew)
        .ok()
        .and_then(|d| now.checked_add(d).ok())
        .unwrap_or(now);
    let mut partitions = Vec::with_capacity(boundaries.len() + 1);
    let mut prev_to = now_skewed;
    for boundary in boundaries {
        // A boundary that runs off the start of the representable range
        // saturates to the same sentinel the final open-ended partition uses.
        let from = SignedDuration::try_from(*boundary)
            .ok()
            .and_then(|d| now_skewed.checked_sub(d).ok())
            .unwrap_or(Timestamp::MIN);
        partitions.push(PartitionBounds::Time { from, to: prev_to });
        prev_to = from;
    }
    // Final open-ended partition: from the minimum representable instant
    // to the oldest boundary. Only the lower bound is a fixed sentinel;
    // every absolute time window still changes with the planning instant.
    partitions.push(PartitionBounds::Time {
        from: Timestamp::MIN,
        to: prev_to,
    });
    PartitionPlan { partitions }
}

fn plan_uid(chunk: u32, total: u32, uid_validity: u32) -> PartitionPlan {
    if total == 0 || chunk == 0 {
        return PartitionPlan { partitions: vec![] };
    }
    let mut partitions = Vec::new();
    // Newest-first: walk high UIDs down.
    let mut to = u64::from(total) + 1;
    let chunk = u64::from(chunk);
    while to > 1 {
        let from = to.saturating_sub(chunk).max(1);
        partitions.push(PartitionBounds::Uid {
            uid_validity,
            from,
            to,
        });
        if from == 1 {
            break;
        }
        to = from;
    }
    PartitionPlan { partitions }
}

fn plan_page(chunk: u32, total: u32) -> PartitionPlan {
    if total == 0 || chunk == 0 {
        return PartitionPlan { partitions: vec![] };
    }
    let mut partitions = Vec::new();
    let mut from: u32 = 0;
    while from < total {
        let to = from.saturating_add(chunk).min(total);
        partitions.push(PartitionBounds::Page { from, to });
        from = to;
    }
    PartitionPlan { partitions }
}

#[cfg(test)]
mod tests {
    use super::*;
    use jiff::civil::date;

    fn now() -> Timestamp {
        date(2026, 5, 21)
            .at(12, 0, 0, 0)
            .in_tz("UTC")
            .unwrap()
            .timestamp()
    }

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

    #[test]
    fn time_windowed_default_partitions_in_order() {
        let policy = BackfillPolicy::default();
        let plan = plan(&policy, now(), 0);
        // 9 boundaries + 1 open-ended tail.
        assert_eq!(plan.partitions.len(), 10);
        // Newest first; each partition's `to` equals the previous
        // partition's `from`.
        for window in plan.partitions.windows(2) {
            let (
                PartitionBounds::Time { from: f0, to: _ },
                PartitionBounds::Time { from: _, to: t1 },
            ) = (&window[0], &window[1])
            else {
                panic!("expected time partitions");
            };
            assert_eq!(f0, t1, "partitions must chain back-to-back");
        }
    }

    #[test]
    fn uid_range_chunks_newest_first() {
        let policy = BackfillPolicy {
            strategy: BackfillStrategy::UidRange {
                uid_validity: 1,
                chunk_size: 1000,
            },
            clock_skew: Duration::ZERO,
        };
        let plan = plan(&policy, now(), 2500);
        assert_eq!(
            plan.partitions,
            vec![
                PartitionBounds::Uid {
                    uid_validity: 1,
                    from: 1501,
                    to: 2501
                },
                PartitionBounds::Uid {
                    uid_validity: 1,
                    from: 501,
                    to: 1501
                },
                PartitionBounds::Uid {
                    uid_validity: 1,
                    from: 1,
                    to: 501
                },
            ]
        );
    }

    #[test]
    fn page_count_walks_in_order() {
        let policy = BackfillPolicy {
            strategy: BackfillStrategy::PageCount {
                items_per_partition: 250,
            },
            clock_skew: Duration::ZERO,
        };
        let plan = plan(&policy, now(), 700);
        assert_eq!(
            plan.partitions,
            vec![
                PartitionBounds::Page { from: 0, to: 250 },
                PartitionBounds::Page { from: 250, to: 500 },
                PartitionBounds::Page { from: 500, to: 700 },
            ]
        );
    }

    #[test]
    fn bounds_convert_to_account_partitions() {
        let time = PartitionBounds::Time {
            from: date(2026, 5, 1)
                .at(0, 0, 0, 0)
                .in_tz("UTC")
                .unwrap()
                .timestamp(),
            to: date(2026, 5, 2)
                .at(0, 0, 0, 0)
                .in_tz("UTC")
                .unwrap()
                .timestamp(),
        };
        assert_eq!(
            inventory_partition_for(&time),
            InventoryPartition::Time(
                bifrost_types::TimeWindow::new(Some(1_777_593_600), Some(1_777_680_000))
                    .expect("ordered window")
            )
        );

        assert_eq!(
            inventory_partition_for(&PartitionBounds::Uid {
                uid_validity: 1,
                from: 10,
                to: 20
            }),
            InventoryPartition::Uid(
                bifrost_types::UidWindow::new(1, 10, 20).expect("ordered window")
            )
        );
        assert_eq!(
            inventory_partition_for(&PartitionBounds::Page { from: 0, to: 50 }),
            InventoryPartition::Page(
                bifrost_types::PageWindow::new(0, 50).expect("ordered window")
            )
        );
    }

    #[test]
    fn partition_keys_are_stable() {
        assert_eq!(
            partition_key(&InventoryPartition::Full),
            BackfillKey::Full.encode()
        );
        assert_eq!(
            partition_key(&InventoryPartition::Page(
                bifrost_types::PageWindow::new(25, 50).expect("ordered window")
            )),
            Partition(b"page:25:50".to_vec())
        );
        assert_eq!(
            partition_key(&InventoryPartition::Uid(
                bifrost_types::UidWindow::new(1, 7, 9).expect("ordered window")
            )),
            Partition(b"uid:1:7:9".to_vec())
        );
        assert_eq!(
            partition_key(&InventoryPartition::Time(
                bifrost_types::TimeWindow::new(None, Some(123)).expect("ordered window")
            )),
            Partition(b"time::123".to_vec())
        );
        for partition in [
            InventoryPartition::Full,
            InventoryPartition::Time(bifrost_types::TimeWindow::new(None, None).unwrap()),
            InventoryPartition::Time(bifrost_types::TimeWindow::new(Some(-456), None).unwrap()),
            InventoryPartition::Time(
                bifrost_types::TimeWindow::new(Some(-456), Some(123)).unwrap(),
            ),
            InventoryPartition::Uid(
                bifrost_types::UidWindow::new(99, 1, u64::from(u32::MAX) + 1).unwrap(),
            ),
            InventoryPartition::Page(bifrost_types::PageWindow::new(0, u32::MAX).unwrap()),
        ] {
            let key = partition_key(&partition);
            assert!(!matches!(BackfillKey::decode(&key), BackfillKey::Opaque(_)));
            assert_eq!(BackfillKey::decode(&key).encode(), key);
        }
    }

    #[test]
    fn page_partition_key_round_trips() {
        let partition = InventoryPartition::Page(
            bifrost_types::PageWindow::new(500, 1000).expect("ordered window"),
        );
        let key = partition_key(&partition);
        assert_eq!(parse_page_partition(&key), Some((500, 1000)));
    }

    #[test]
    fn parse_page_partition_rejects_other_kinds() {
        assert_eq!(
            parse_page_partition(&partition_key(&InventoryPartition::Uid(
                bifrost_types::UidWindow::new(1, 7, 9).expect("ordered window")
            ))),
            None
        );
        assert_eq!(
            parse_page_partition(&partition_key(&InventoryPartition::Full)),
            None
        );
        assert_eq!(parse_page_partition(&Partition(b"page:1".to_vec())), None);
        assert_eq!(parse_page_partition(&Partition(b"page:a:b".to_vec())), None);
    }

    #[test]
    fn completion_sentinel_is_distinct_from_pages() {
        let complete = completion_partition();
        assert!(is_completion_partition(&complete));
        assert_eq!(parse_page_partition(&complete), None);
        assert!(!is_completion_partition(&partition_key(
            &InventoryPartition::Page(
                bifrost_types::PageWindow::new(0, 500).expect("ordered window")
            )
        )));
    }

    #[test]
    fn empty_inputs_yield_empty_plan() {
        let policy = BackfillPolicy {
            strategy: BackfillStrategy::UidRange {
                uid_validity: 1,
                chunk_size: 100,
            },
            clock_skew: Duration::ZERO,
        };
        assert!(plan(&policy, now(), 0).partitions.is_empty());
        let policy = BackfillPolicy {
            strategy: BackfillStrategy::PageCount {
                items_per_partition: 50,
            },
            clock_skew: Duration::ZERO,
        };
        assert!(plan(&policy, now(), 0).partitions.is_empty());
    }
}
