//! Backfill partition planner tests.
//!
//! Covers the three partitioning strategies plus deterministic
//! boundary chaining for the time-windowed default.

use std::time::Duration;

use bifrost_sync::backfill::partitioner::PartitionBounds;
use bifrost_sync::backfill::{
    BackfillPolicy, BackfillStrategy, default_time_boundaries, partitioner,
};
use jiff::Timestamp;
use jiff::civil::date;

fn fixed_now() -> Timestamp {
    date(2026, 5, 21)
        .at(12, 0, 0, 0)
        .in_tz("UTC")
        .unwrap()
        .timestamp()
}

#[test]
fn default_time_partitions_chain_back_to_back() {
    let policy = BackfillPolicy::default();
    let plan = partitioner::plan(&policy, fixed_now(), 0);
    let n_boundaries = default_time_boundaries().len();
    // n_boundaries explicit partitions + 1 open-ended tail.
    assert_eq!(plan.partitions.len(), n_boundaries + 1);
    for window in plan.partitions.windows(2) {
        match (&window[0], &window[1]) {
            (PartitionBounds::Time { from: f0, .. }, PartitionBounds::Time { to: t1, .. }) => {
                assert_eq!(f0, t1, "partitions must chain back-to-back");
            }
            _ => panic!("expected time partitions"),
        }
    }
}

#[test]
fn uid_range_plan_is_newest_first_and_complete() {
    let policy = BackfillPolicy {
        strategy: BackfillStrategy::UidRange {
            uid_validity: 1,
            chunk_size: 1000,
        },
        clock_skew: Duration::ZERO,
    };
    let plan = partitioner::plan(&policy, fixed_now(), 4500);
    // 4500 / 1000 = 4.5 -> 5 partitions, newest first
    assert_eq!(plan.partitions.len(), 5);
    let first = &plan.partitions[0];
    let last = &plan.partitions[plan.partitions.len() - 1];
    match first {
        PartitionBounds::Uid {
            uid_validity: _,
            from: _,
            to,
        } => assert_eq!(*to, 4501),
        _ => panic!("expected UID partition"),
    }
    match last {
        PartitionBounds::Uid {
            uid_validity: _,
            from,
            to: _,
        } => assert_eq!(*from, 1),
        _ => panic!("expected UID partition"),
    }
}

#[test]
fn page_count_plan_walks_in_order() {
    let policy = BackfillPolicy {
        strategy: BackfillStrategy::PageCount {
            items_per_partition: 100,
        },
        clock_skew: Duration::ZERO,
    };
    let plan = partitioner::plan(&policy, fixed_now(), 350);
    assert_eq!(
        plan.partitions,
        vec![
            PartitionBounds::Page { from: 0, to: 100 },
            PartitionBounds::Page { from: 100, to: 200 },
            PartitionBounds::Page { from: 200, to: 300 },
            PartitionBounds::Page { from: 300, to: 350 },
        ]
    );
}

#[test]
fn clock_skew_shifts_time_partitions_forward() {
    let policy_no_skew = BackfillPolicy::default();
    let policy_skew = BackfillPolicy {
        clock_skew: Duration::from_secs(60 * 60), // server one hour ahead
        ..BackfillPolicy::default()
    };
    let no_skew = partitioner::plan(&policy_no_skew, fixed_now(), 0);
    let with_skew = partitioner::plan(&policy_skew, fixed_now(), 0);
    // Each `to` boundary in the skewed plan should be exactly one
    // hour ahead of the non-skewed plan's matching boundary.
    let expected_delta = jiff::SignedDuration::from_hours(1);
    for (lhs, rhs) in no_skew.partitions.iter().zip(with_skew.partitions.iter()) {
        match (lhs, rhs) {
            (
                PartitionBounds::Time { from: f0, to: t0 },
                PartitionBounds::Time { from: f1, to: t1 },
            ) => {
                // Both ends shift by the skew.
                assert_eq!(t1.duration_since(*t0), expected_delta);
                if *f0 != Timestamp::MIN {
                    assert_eq!(f1.duration_since(*f0), expected_delta);
                }
            }
            _ => panic!("expected time partitions"),
        }
    }
}

#[test]
fn small_total_uid_yields_one_partition() {
    let policy = BackfillPolicy {
        strategy: BackfillStrategy::UidRange {
            uid_validity: 1,
            chunk_size: 5000,
        },
        clock_skew: Duration::ZERO,
    };
    let plan = partitioner::plan(&policy, fixed_now(), 12);
    assert_eq!(
        plan.partitions,
        vec![PartitionBounds::Uid {
            uid_validity: 1,
            from: 1,
            to: 13
        }]
    );
}

#[test]
fn empty_total_yields_no_partitions() {
    let policy = BackfillPolicy {
        strategy: BackfillStrategy::UidRange {
            uid_validity: 1,
            chunk_size: 100,
        },
        clock_skew: Duration::ZERO,
    };
    let plan = partitioner::plan(&policy, fixed_now(), 0);
    assert!(plan.partitions.is_empty());
}
