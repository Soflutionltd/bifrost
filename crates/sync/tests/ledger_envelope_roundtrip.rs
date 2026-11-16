//! Durable `DebtLedger` encode/decode.
//!
//! Every assertion here is about something a restart must not forget. The
//! ledger is the record of what an account still owes and what an operator
//! decided about it, so a field that silently fails to round-trip does not
//! produce a visible error - it produces a scope that comes back looking clean.
//!
//! `DebtLedger` is not `PartialEq` and deliberately has no public constructor,
//! so these tests build state the way the engine does (fold reports, record
//! barriers, take operator decisions) and compare the restored ledger
//! field-by-field. The retained proof set is private, so it is checked
//! BEHAVIOURALLY: a union proof that only lands after restore is the only way
//! to observe that the earlier half survived.

use bifrost_sync::cursor::ledger_envelope::{
    LEDGER_ENVELOPE_VERSION, decode_ledger, encode_ledger,
};
use bifrost_sync::{
    BarrierIncident, BarrierPolicy, DebtLedger, DischargeEvidence, EntryPolicy, LedgerEntry,
    ProofStatus, UnixSeconds,
};
use bifrost_types::{
    AccountError, AccountErrorBuilder, AccountOperation, AuthCause, ChangeCursor, Checkpoint,
    CoverageCoordinate, CoverageDomain, CursorScope, DiagnosticText, ErrorScope,
    InventoryCoverageReport, InventoryObligation, InventoryRepairTarget, ObjectId, ObjectType,
    ObligationKey, OpaqueChangeState, PrimaryCause, Protocol, ProtocolKind, Provider,
    RegionRecovery, RepairAttemptId, RequestCause, SnapshotIdentity,
};

fn scope() -> CursorScope {
    CursorScope::Type(ObjectType::Email)
}

fn key(name: &str) -> ObligationKey {
    ObligationKey(name.as_bytes().to_vec())
}

/// A richly-decorated error, so the digest is exercised on more than a bare
/// kind: scope, operation, provider, protocol, telemetry tokens and both
/// diagnostic visibility tiers all have to come back.
fn decorated_error() -> AccountError {
    AccountErrorBuilder::new(PrimaryCause::Request(RequestCause::Malformed {
        detail: DiagnosticText::support_only("unrepresentable"),
    }))
    .scope(ErrorScope::Message {
        id: ObjectId("m-1".into()),
    })
    .operation(AccountOperation::SyncInventory)
    .provider(Provider::Microsoft)
    .protocol(Protocol::Graph)
    .status(Some(422))
    .request_id("req-77")
    .trace_id("trace-77")
    .native_code("ErrorInvalidItem")
    .text(DiagnosticText::user_safe("A message could not be read."))
    .text(DiagnosticText::support_only("raw provider body"))
    .try_build()
    .expect("valid account error classification")
}

/// A second, structurally different error, so the two are told apart rather
/// than both matching one lenient assertion.
fn auth_error() -> AccountError {
    AccountErrorBuilder::new(PrimaryCause::Auth(AuthCause::Revoked))
        .operation(AccountOperation::Expunge)
        .try_build()
        .expect("valid account error classification")
}

fn object(name: &str) -> InventoryObligation {
    InventoryObligation::Object {
        key: key(name),
        id: ObjectId(name.to_string()),
        error: decorated_error(),
        repair: name.as_bytes().to_vec(),
    }
}

fn replayable(name: &str) -> InventoryObligation {
    InventoryObligation::Region {
        key: key(name),
        failure_label: "truncated".into(),
        error: auth_error(),
        recovery: RegionRecovery::DurableReplay {
            token: b"replay-token".to_vec(),
        },
    }
}

fn barrier_region(name: &str) -> InventoryObligation {
    InventoryObligation::Region {
        key: key(name),
        failure_label: "unidentifiable-value".into(),
        error: auth_error(),
        recovery: RegionRecovery::barrier(),
    }
}

fn domain(coordinate: CoverageCoordinate, snapshot: SnapshotIdentity) -> CoverageDomain {
    CoverageDomain {
        scope: scope(),
        coordinate,
        snapshot,
    }
}

fn time_domain(from: i64, to: i64) -> CoverageDomain {
    domain(
        CoverageCoordinate::TimeRange(
            bifrost_types::TimeWindow::new(Some(from), Some(to)).expect("ordered window"),
        ),
        SnapshotIdentity::unstable(),
    )
}

fn checkpoint() -> Checkpoint {
    Checkpoint::Change(ChangeCursor {
        scope: scope(),
        server_state: OpaqueChangeState {
            protocol: ProtocolKind::Graph,
            envelope_version: bifrost_types::PayloadVersion::new(3),
            bytes: b"delta-token".to_vec(),
        },
        advanced_through: None,
        envelope_version: bifrost_types::TraitEnvelopeVersion::new(
            bifrost_types::CHANGE_CURSOR_ENVELOPE_VERSION,
        ),
    })
}

// ---------- comparison helpers ----------

fn assert_errors_match(
    left: &bifrost_sync::cursor::ErrorDigest,
    right: &bifrost_sync::cursor::ErrorDigest,
    what: &str,
) {
    assert_eq!(left.kind, right.kind, "{what}: kind");
    assert_eq!(left.scope, right.scope, "{what}: scope");
    assert_eq!(left.operation, right.operation, "{what}: operation");
    assert_eq!(left.provider, right.provider, "{what}: provider");
    assert_eq!(left.protocol, right.protocol, "{what}: protocol");
    assert_eq!(
        left.diagnostics.status, right.diagnostics.status,
        "{what}: status"
    );
    assert_eq!(
        left.diagnostics.request_id, right.diagnostics.request_id,
        "{what}: request id"
    );
    assert_eq!(
        left.diagnostics.trace_id, right.diagnostics.trace_id,
        "{what}: trace id"
    );
    assert_eq!(
        left.diagnostics.native_code, right.diagnostics.native_code,
        "{what}: native code"
    );
    assert_eq!(
        left.diagnostics.text, right.diagnostics.text,
        "{what}: text"
    );
}

fn assert_proof_matches(left: &ProofStatus, right: &ProofStatus, what: &str) {
    match (left, right) {
        (ProofStatus::Unresolved, ProofStatus::Unresolved) => {}
        (
            ProofStatus::Discharged { evidence: left },
            ProofStatus::Discharged { evidence: right },
        ) => match (left, right) {
            (
                DischargeEvidence::CoveringWalk { domain: left },
                DischargeEvidence::CoveringWalk { domain: right },
            ) => assert_eq!(left, right, "{what}: covering-walk domain"),
            (
                DischargeEvidence::RepairedAndPublished { attempt: left },
                DischargeEvidence::RepairedAndPublished { attempt: right },
            ) => assert_eq!(left, right, "{what}: repair attempt"),
            (
                DischargeEvidence::ProvedIrrelevant { detail: left },
                DischargeEvidence::ProvedIrrelevant { detail: right },
            ) => assert_eq!(left, right, "{what}: irrelevance detail"),
            (
                DischargeEvidence::ReplacedByChildren { children: left },
                DischargeEvidence::ReplacedByChildren { children: right },
            ) => assert_eq!(left, right, "{what}: replacement children"),
            (left, right) => {
                panic!("{what}: discharge evidence changed shape: {left:?} vs {right:?}")
            }
        },
        (left, right) => panic!("{what}: proof changed shape: {left:?} vs {right:?}"),
    }
}

fn assert_entries_match(left: &LedgerEntry, right: &LedgerEntry) {
    let what = format!("entry {:?}", String::from_utf8_lossy(&left.key.0));
    assert_eq!(left.key, right.key, "{what}: key");
    assert_eq!(left.domain, right.domain, "{what}: domain");
    assert_eq!(left.generation, right.generation, "{what}: generation");
    assert_eq!(left.policy, right.policy, "{what}: policy");
    assert_eq!(left.target, right.target, "{what}: repair target");
    assert_eq!(left.parent, right.parent, "{what}: lineage parent");
    assert_eq!(
        left.first_seen_unix_seconds, right.first_seen_unix_seconds,
        "{what}: first seen"
    );
    assert_proof_matches(&left.proof, &right.proof, &what);
    assert_errors_match(&left.last_error, &right.last_error, &what);
}

fn assert_barriers_match(left: &BarrierIncident, right: &BarrierIncident) {
    let what = format!("barrier {:?}", String::from_utf8_lossy(&left.key.0));
    assert_eq!(left.key, right.key, "{what}: key");
    assert_eq!(left.domain, right.domain, "{what}: domain");
    assert_eq!(left.generation, right.generation, "{what}: generation");
    assert_eq!(
        left.failure_label, right.failure_label,
        "{what}: failure label"
    );
    assert_eq!(left.policy, right.policy, "{what}: policy");
    assert_eq!(left.resume_from, right.resume_from, "{what}: resume point");
    assert_errors_match(&left.evidence, &right.evidence, &what);
}

fn assert_ledgers_match(original: &DebtLedger, restored: &DebtLedger) {
    let left: Vec<&LedgerEntry> = original.entries().collect();
    let right: Vec<&LedgerEntry> = restored.entries().collect();
    assert_eq!(left.len(), right.len(), "entry count");
    for (left, right) in left.iter().zip(right.iter()) {
        assert_entries_match(left, right);
    }

    let left: Vec<&BarrierIncident> = original.barriers().collect();
    let right: Vec<&BarrierIncident> = restored.barriers().collect();
    assert_eq!(left.len(), right.len(), "barrier count");
    for (left, right) in left.iter().zip(right.iter()) {
        assert_barriers_match(left, right);
    }
}

fn roundtrip(ledger: &DebtLedger) -> DebtLedger {
    decode_ledger(&encode_ledger(ledger)).expect("a ledger this engine wrote must decode")
}

// ---------- the exhaustive ledger ----------

/// Build a ledger that reaches every variant of every enum the codec has a tag
/// for, with both maps non-empty. Assembled through the engine's own mutation
/// API, because that is the only way ledger state is ever produced - a ledger
/// hand-built through a back door could exercise combinations the engine never
/// creates while missing the ones it does.
fn exhaustive_ledger() -> DebtLedger {
    let mut ledger = DebtLedger::new();

    // Retrying / Unresolved / Object target, on a UID-range domain.
    ledger.ingest(
        &InventoryCoverageReport::degraded(
            domain(
                CoverageCoordinate::UidRange(
                    bifrost_types::UidWindow::new(9, 100, 400).expect("ordered window"),
                ),
                SnapshotIdentity(Some(b"snap-uid".to_vec())),
            ),
            vec![object("retrying-object")],
        ),
        walk_generation(7),
        1_000,
    );

    // Waived, on a page-range domain with a stable snapshot.
    ledger.ingest(
        &InventoryCoverageReport::degraded(
            domain(
                CoverageCoordinate::PageRange(
                    bifrost_types::PageWindow::new(0, 50).expect("ordered window"),
                ),
                SnapshotIdentity(Some(b"snap-page".to_vec())),
            ),
            vec![object("waived-object")],
        ),
        walk_generation(7),
        1_100,
    );
    assert!(ledger.waive(&key("waived-object"), "operator@example".into(), 1_200));

    // OperatorBlocked, on a provider-region domain.
    ledger.ingest(
        &InventoryCoverageReport::degraded(
            domain(
                CoverageCoordinate::ProviderRegion {
                    namespace: "graph".into(),
                    region: b"shard-3".to_vec(),
                },
                SnapshotIdentity::unstable(),
            ),
            vec![object("blocked-object")],
        ),
        walk_generation(7),
        1_300,
    );
    assert!(ledger.block(&key("blocked-object")));

    // Discharged by RepairedAndPublished, with a Region target.
    ledger.ingest(
        &InventoryCoverageReport::degraded(
            CoverageDomain::full(scope()),
            vec![replayable("repaired-region")],
        ),
        walk_generation(7),
        1_400,
    );
    assert!(ledger.discharge_repaired(
        &key("repaired-region"),
        walk_generation(7),
        DischargeEvidence::RepairedAndPublished {
            attempt: RepairAttemptId(42),
        },
    ));

    // Discharged by ProvedIrrelevant.
    ledger.ingest(
        &InventoryCoverageReport::degraded(
            CoverageDomain::full(scope()),
            vec![object("irrelevant-object")],
        ),
        walk_generation(7),
        1_500,
    );
    assert!(ledger.discharge_repaired(
        &key("irrelevant-object"),
        walk_generation(7),
        DischargeEvidence::ProvedIrrelevant {
            detail: "absent under cursor bridge".into(),
        },
    ));

    // Discharged by ReplacedByChildren, and a child carrying a lineage parent.
    ledger.ingest(
        &InventoryCoverageReport::degraded(
            CoverageDomain::full(scope()),
            vec![replayable("replaced-parent")],
        ),
        walk_generation(7),
        1_600,
    );
    ledger
        .replace_obligation(
            &key("replaced-parent"),
            &[],
            &[object("child-one"), object("child-two")],
            walk_generation(7),
            1_700,
        )
        .expect("replacement accepted");

    // A barrier crossed by an operator waiver, which is the only path to an
    // entry with NO repair target.
    let crossable = InventoryCoverageReport::degraded(
        CoverageDomain::full(scope()),
        vec![barrier_region("crossed-barrier")],
    );
    ledger.record_barrier(BarrierIncident {
        key: key("crossed-barrier"),
        domain: CoverageDomain::full(scope()),
        generation: walk_generation(7),
        failure_label: "unidentifiable-value".into(),
        evidence: auth_error().into(),
        policy: BarrierPolicy::Undecided,
        resume_from: None,
    });
    assert!(ledger.waive(&key("crossed-barrier"), "operator@example".into(), 1_800));
    assert!(ledger.cross_waived_barriers(&crossable, walk_generation(7), 1_900));

    // Two live barriers: one with a resume point, one without, and different
    // policies so both halves of the barrier codec are exercised.
    ledger.record_barrier(BarrierIncident {
        key: key("resumable-barrier"),
        domain: time_domain(0, 500),
        generation: walk_generation(8),
        failure_label: "page-boundary-lost".into(),
        evidence: decorated_error().into(),
        policy: BarrierPolicy::Undecided,
        resume_from: Some(checkpoint()),
    });
    ledger.record_barrier(BarrierIncident {
        key: key("blocked-barrier"),
        domain: CoverageDomain::full(scope()),
        generation: walk_generation(8),
        failure_label: "unidentifiable-value".into(),
        evidence: auth_error().into(),
        policy: BarrierPolicy::Undecided,
        resume_from: None,
    });
    assert!(ledger.block(&key("blocked-barrier")));

    // Debt on a time window, plus HALF the union that would discharge it. The
    // proof is retained because it can still contribute; the other half arrives
    // after the round trip.
    ledger.ingest(
        &InventoryCoverageReport::degraded(time_domain(30, 90), vec![object("union-debt")]),
        walk_generation(9),
        2_000,
    );
    ledger.ingest(
        &InventoryCoverageReport::complete(time_domain(7, 60)),
        walk_generation(10),
        2_100,
    );

    ledger
}

// ---------- tests ----------

#[test]
fn a_fully_populated_ledger_round_trips() {
    let ledger = exhaustive_ledger();
    assert!(ledger.entries().count() >= 8, "entries must be non-empty");
    assert!(ledger.barriers().count() >= 2, "barriers must be non-empty");

    assert_ledgers_match(&ledger, &roundtrip(&ledger));
}

/// Encoding is deterministic, which is what lets a backend compare a stored
/// blob against a freshly encoded one without decoding it, and what makes a
/// diff of two dumps mean something. The ordered maps are the reason it holds.
#[test]
fn encoding_is_deterministic_and_stable_across_a_round_trip() {
    let ledger = exhaustive_ledger();
    let once = encode_ledger(&ledger);
    assert_eq!(once, encode_ledger(&ledger), "same ledger, same bytes");
    assert_eq!(
        once,
        encode_ledger(&roundtrip(&ledger)),
        "a decoded ledger must re-encode to the bytes it came from"
    );
}

/// The ledger's decisions have to survive as DECISIONS, not just as data. This
/// asks the restored ledger the questions the engine actually asks it.
#[test]
fn a_restored_ledger_answers_the_engine_the_same_way() {
    let ledger = exhaustive_ledger();
    let restored = roundtrip(&ledger);

    assert_eq!(
        restored.open_debt().count(),
        ledger.open_debt().count(),
        "open debt must not change across a restart"
    );
    assert_eq!(
        restored.repairable().count(),
        ledger.repairable().count(),
        "what is eligible for automatic repair must not change"
    );
    assert_eq!(
        restored.completion_permitted(&scope()),
        ledger.completion_permitted(&scope()),
    );
    assert!(
        !restored.completion_permitted(&scope()),
        "open debt and a blocked barrier both still hold the sentinel"
    );
    assert!(
        restored.scope_has_blocked_barrier(&scope()),
        "an operator's block must park the rescan after a restart too"
    );
    assert!(
        restored.barrier_waived(&key("crossed-barrier"))
            || restored.entry(&key("crossed-barrier")).is_some(),
        "the crossed barrier survives as accepted-loss debt"
    );
    assert_eq!(
        restored.lineage_root(&key("child-one")),
        key("replaced-parent"),
        "a retry budget follows the lineage, so the parent link is load-bearing"
    );
}

/// A waiver is accepted loss, never proof. If a restart turned one into the
/// other the audit trail would start claiming coverage nobody proved - the
/// single failure this whole subsystem exists to prevent.
#[test]
fn a_waiver_survives_as_accepted_loss_and_not_as_proof() {
    let restored = roundtrip(&exhaustive_ledger());
    let entry = restored.entry(&key("waived-object")).expect("waived entry");

    assert_eq!(
        entry.proof,
        ProofStatus::Unresolved,
        "a waiver proves nothing, before or after a restart"
    );
    assert!(entry.policy.is_waived());
    assert_eq!(
        entry.policy,
        EntryPolicy::Waived {
            by: "operator@example".into(),
            at_unix_seconds: UnixSeconds(1_200),
        },
        "who waived it and when is the whole content of the decision"
    );
    assert!(!entry.blocks_completion());
}

/// The retry budget accrues at the lineage root and must not reset on restart.
/// An account that could reset a budget by outliving a process restart evades
/// every budget the ledger has.
#[test]
fn a_spent_retry_budget_survives_a_restart() {
    let mut ledger = DebtLedger::new();
    ledger.ingest(
        &InventoryCoverageReport::degraded(CoverageDomain::full(scope()), vec![object("budgeted")]),
        walk_generation(1),
        100,
    );
    assert!(!ledger.record_attempt(&key("budgeted"), 3));
    assert!(!ledger.record_attempt(&key("budgeted"), 3));

    let mut restored = roundtrip(&ledger);
    assert_eq!(
        restored
            .lineage_budget(&key("budgeted"))
            .expect("budget")
            .attempts,
        2
    );
    assert_eq!(
        restored.entry(&key("budgeted")).expect("entry").policy,
        EntryPolicy::Automatic,
        "two completed attempts must still be charged after a restart"
    );
    assert!(
        restored.record_attempt(&key("budgeted"), 3),
        "the third attempt must exhaust the budget, not the fifth"
    );
    assert_eq!(
        restored.entry(&key("budgeted")).expect("entry").policy,
        EntryPolicy::OperatorBlocked,
    );
}

/// Retained proofs are private state with no accessor, so the only honest test
/// is behavioural: debt discharged by the UNION of two windows must still
/// discharge when the first window was proved before the restart and the second
/// after it. Drop the proof set from the codec and this is the test that fails,
/// because the surviving half is invisible to any field comparison.
#[test]
fn a_retained_proof_still_completes_a_union_after_a_restart() {
    let mut restored = roundtrip(&exhaustive_ledger());
    assert!(
        restored.entry(&key("union-debt")).expect("entry").is_open(),
        "half a union proves nothing yet"
    );

    restored.ingest(
        &InventoryCoverageReport::complete(time_domain(60, 180)),
        walk_generation(11),
        3_000,
    );

    assert!(
        matches!(
            restored.entry(&key("union-debt")).expect("entry").proof,
            ProofStatus::Discharged { .. }
        ),
        "the pre-restart half of the union must still count toward the proof"
    );
}

/// A repair descriptor is the account-minted material a repair pass needs; the
/// error is explicitly not usable for that. So the target has to come back
/// exactly, both shapes of it.
#[test]
fn repair_targets_survive_in_both_shapes() {
    let restored = roundtrip(&exhaustive_ledger());

    assert_eq!(
        restored
            .entry(&key("retrying-object"))
            .expect("object entry")
            .target,
        Some(InventoryRepairTarget::Object {
            id: ObjectId("retrying-object".into()),
            repair: b"retrying-object".to_vec(),
        }),
    );
    assert_eq!(
        restored
            .entry(&key("repaired-region"))
            .expect("region entry")
            .target,
        Some(InventoryRepairTarget::Region {
            replay: b"replay-token".to_vec(),
        }),
    );
    assert_eq!(
        restored
            .entry(&key("crossed-barrier"))
            .expect("crossed barrier entry")
            .target,
        None,
        "a crossed barrier has no repair path; inventing one would be worse than none"
    );
}

/// A barrier's resume point is where a later walk picks up. It nests a whole
/// cursor envelope, so this is also the check that the two codecs compose.
#[test]
fn a_barrier_resume_checkpoint_nests_a_cursor_envelope() {
    let restored = roundtrip(&exhaustive_ledger());
    let barrier = restored
        .barriers()
        .find(|barrier| barrier.key == key("resumable-barrier"))
        .expect("resumable barrier");

    assert_eq!(barrier.resume_from, Some(checkpoint()));
    assert_eq!(barrier.policy, BarrierPolicy::Undecided);
}

/// An unreadable durable row must be classified as a SCHEMA problem, in both
/// directions, because that is the only classification a consumer's healing
/// path keys on. Corrupt framing must not acquire the same authority: clearing
/// a ledger is not the right answer to a flipped bit.
#[test]
fn an_out_of_window_version_is_schema_incompatible_and_corruption_is_not() {
    let encoded = encode_ledger(&exhaustive_ledger());

    for version in [0u32, LEDGER_ENVELOPE_VERSION + 1, u32::MAX] {
        let mut bytes = encoded.clone();
        bytes[4..8].copy_from_slice(&version.to_le_bytes());
        assert!(
            matches!(
                decode_ledger(&bytes),
                Err(bifrost_sync::error::Error::SchemaIncompatible)
            ),
            "version {version} must be schema-incompatible"
        );
    }

    let mut corrupt = encoded.clone();
    corrupt[0] ^= 0xFF;
    assert!(
        matches!(
            decode_ledger(&corrupt),
            Err(bifrost_sync::error::Error::Other(_))
        ),
        "bad magic is corruption, not a schema signal"
    );

    let truncated = &encoded[..encoded.len() - 1];
    assert!(
        decode_ledger(truncated).is_err(),
        "a truncated ledger must be refused, not silently shortened"
    );
}

// Mint test proof orders through the same owner as production walks.
fn walk_generation(value: u64) -> bifrost_sync::cursor::WalkGeneration {
    let coverage = bifrost_sync::cursor::PendingCoverage::new();
    for _ in 0..value {
        let _ = coverage.next_generation();
    }
    coverage.next_generation()
}
