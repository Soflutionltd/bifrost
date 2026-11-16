//! Writer operations: acknowledge.

use super::*;

impl Writer {
    pub(super) async fn ack(&mut self, req: AckRequest) {
        let account_id = self.account_id.clone();
        let store = Arc::clone(&self.store);
        let control = self.control.clone();
        let coverage = Arc::clone(&self.coverage);
        let ledger = self.ledger.as_mut().expect("ledger loaded before dispatch");
        let provisional = &mut self.provisional.rows;

        if let Err(error) = validate_ack_scope(&req.scope, &req.checkpoint) {
            if let Some(done) = req.complete {
                let _ = done.send(Err(error));
            }
            return;
        }
        // No lane bookkeeping here, deliberately. Backfill capacity IS the
        // publication's boundary registration, so each arm below frees it by
        // doing what it already does to the ledger: `record_publication`
        // acknowledges, `retire_publication` retires. An unconditional release
        // beside them was a defect rather than a shortcut - it fired on the
        // REJECTED arm too, where the consumer's token named a different lane
        // from its checkpoint, and released a live sibling publication's
        // capacity on the strength of an acknowledgement the ledger had just
        // refused.
        let result = persist_ack_request(
            &account_id,
            &store,
            &coverage,
            ledger,
            &req,
            &mut self.completed_backfills,
        )
        .await;
        match result {
            Ok(AckPersistOutcome::Withheld(reason)) => {
                // The completion sentinel was deliberately NOT written: the
                // scope still carries open debt, so only the ledger landed.
                // Nothing durable was created for this publication, so the
                // watermark must not move (a retried ack has to report
                // `Unknown` and be re-evaluated, never `AlreadyPersisted`) and
                // the boundary must not be announced durable - the store holds
                // no such row and the next attach re-walks. The publication is
                // retired instead, exactly as a failed write retires it, so
                // boundary waiters stop being gated. The consumer's ack itself
                // succeeded: its batch was empty and was honoured.
                if let Some(publication) = req.publication {
                    control.retire_publication(publication);
                }
                if let Some(done) = req.complete {
                    let _ = done.send(Ok(AckOutcome::Withheld(reason)));
                }
            }
            Ok(AckPersistOutcome::Durable) => {
                if let Checkpoint::Backfill(checkpoint) = &req.checkpoint
                    && proves_backfill_completion(checkpoint)
                {
                    self.completed_backfills.insert(req.scope.clone());
                }
                // Only a durable change cursor promotes the provisional row.
                // Rejected, withheld and failed writes leave it abort-owned;
                // a backfill write does not commit the change cursor at all.
                if matches!(&req.checkpoint, Checkpoint::Change(_)) {
                    provisional.remove(&req.scope);
                }
                // Notify pause / checkpoint_now waiters AFTER the
                // durable write lands - the contract is that the
                // returned checkpoint has been persisted. The
                // watermark moves here for the same reason: a retried
                // acknowledgement may only report "already persisted"
                // for a write that actually landed.
                if let Some(publication) = req.publication.clone() {
                    coverage.settle_checkpoint(publication, &req.checkpoint);
                }
                control
                    .record_publication(req.publication, req.checkpoint)
                    .await;
                if let Some(done) = req.complete {
                    let _ = done.send(Ok(AckOutcome::Durable));
                }
            }
            Err(AckFailure::Store(err)) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?account_id,
                    scope = ?req.scope,
                    error = %err,
                    "ack: checkpoint persist failed"
                );
                // The batch is no longer in flight - the consumer took
                // delivery and the engine has done all it can - so it
                // must stop gating boundary waiters even though it
                // never became durable. Leaving it outstanding would
                // wedge every later pause / checkpoint_now on this
                // account. The consumer learns about the store failure
                // from `ack`'s own `Result`, below. The
                // watermark deliberately does NOT move: a retry of
                // this acknowledgement must report `Unknown` and fail
                // again, never "already persisted" for a write that
                // never landed.
                if let Some(publication) = req.publication {
                    control.retire_publication(publication);
                }
                if let Some(done) = req.complete {
                    let _ = done.send(Err(Error::AckStore(Box::new(err))));
                }
            }
            Err(AckFailure::Rejected(err)) => {
                tracing::warn!(
                    target: "bifrost.sync.changes",
                    account = ?account_id,
                    scope = ?req.scope,
                    error = %err,
                    "ack: acknowledgement refused before any durable write"
                );
                // NOTHING is retired here, and the distinction from a store
                // failure is the point. A store failure is evidence the consumer
                // took delivery and answered; a REJECTION is evidence the
                // acknowledgement did not name what it claimed to - a token from
                // one lane presented with another lane's checkpoint, or an
                // undecodable envelope. Acting on it would retire, and so free
                // the capacity of, a publication that is still perfectly live and
                // still awaiting its own acknowledgement.
                if let Some(done) = req.complete {
                    let _ = done.send(Err(err));
                }
            }
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum AckPersistOutcome {
    Durable,
    Withheld(CompletionRefusal),
}

/// Why an acknowledgement produced nothing durable.
///
/// Two cases that look alike and must not be treated alike. `Store` means the
/// consumer's acknowledgement was valid and the write failed, so the batch is
/// no longer in flight and its registration must be retired. `Rejected` means
/// the acknowledgement never named a publication this ledger would honour - a
/// mismatched lane, an undecodable envelope - so it is evidence about the
/// CALLER, not about delivery, and retiring anything on the strength of it
/// would free a live publication's boundary and its backfill capacity.
#[derive(Debug)]
pub(super) enum AckFailure {
    Rejected(Error),
    Store(Error),
}

/// Why this completion sentinel must not become durable, if it must not.
///
/// Two independent questions, and the second is the one a point-in-time check
/// cannot answer. The ledger knows whether the scope carries open debt. It does
/// NOT know whether every page of the walk that produced this marker reached a
/// consumer: a page that was CLEAN and simply never persisted owes nothing, so
/// the debt check waves it through. The marker therefore carries the walk's
/// undelivered watermark from the moment it was published, and it is compared
/// again here - the same `Withheld` answer the emission gives, one layer later,
/// because with spare capacity the marker is published before the loss happens.
/// The reader that departs and strands a page is not required to do so before
/// the marker goes out; it only has to do so before the replacement acknowledges
/// it, and the retirement warning arrives BEHIND the marker in the same ring.
///
/// A marker whose registration is already gone reports no watermark, and that is
/// refused too: an abandoned or evicted marker is precisely the case where
/// delivery cannot be vouched for. So is an acknowledgement carrying no
/// publication at all, which skips the reading, the reset fence and the claim
/// lookup alike.
fn completion_refusal(
    coverage: &PendingCoverage,
    ledger: &crate::cursor::DebtLedger,
    checkpoint: &bifrost_types::BackfillCheckpoint,
    publication: Option<&crate::cursor::AckToken>,
) -> Option<CompletionRefusal> {
    // Evaluated FIRST and independently of the debt, because the two answers
    // call for different repairs and the stronger one must not be masked. When
    // both hold, an early `OpenDebt` return withholds the marker and leaves the
    // walk's positional rows - which point past the page nobody received -
    // sitting in the store, so a detach before the in-memory retry hands a fresh
    // attach a resume position beyond the hole. Debt is a property of the scope;
    // this is a property of the walk.
    // The watermark rides the RECEIPT, so "absent" does not mean "the
    // registration is gone": a marker whose store write failed once, or whose
    // entry a later publication superseded, still carries its reading and is
    // still judged here. What is left over is an acknowledgement replayed across
    // a detach, and that one is `Unvouchable` - withheld, with nothing touched.
    // A completion sentinel acknowledged with NO publication at all is refused
    // too, and for the same reason: with no id there is no reading, no reset
    // fence and no claim lookup, so every check that stands between an
    // acknowledgement and a durable completion is skipped and the marker becomes
    // durable on the caller's say-so alone. Nothing in the engine acknowledges a
    // completion marker without its publication - the live driver refuses a
    // backfill checkpoint outright, and the orchestrator always publishes one -
    // so this is reachable only from a consumer that dropped the id, which is the
    // case that must not settle a scope for good.
    let Some(publication) = publication else {
        return Some(CompletionRefusal::Unvouchable);
    };
    match coverage.walk_watermark(publication) {
        Some(recorded) if recorded != coverage.undelivered_watermark(&checkpoint.scope) => {
            return Some(CompletionRefusal::WalkNotWhole);
        }
        None => return Some(CompletionRefusal::Unvouchable),
        Some(_) => {}
    }
    if !ledger.completion_permitted(&checkpoint.scope) {
        return Some(CompletionRefusal::OpenDebt);
    }
    None
}

/// Three reasons, kept apart because they call for different repairs.
///
/// `OpenDebt` is about the scope's obligations and says nothing about the walk:
/// the rows it produced are still a valid resume position, and a later pass may
/// earn the marker once the debt is discharged.
///
/// `WalkNotWhole` says the walk itself is void - its pages did not all reach a
/// consumer - so the rows it left are a resume position PAST a hole and must go,
/// together with the outstanding publications of the attempt that wrote them.
///
/// `Unvouchable` says this ledger cannot answer the question at all: the marker
/// was minted by a PRIOR attachment, so the reading its receipt carries belongs
/// to a per-scope counter that no longer exists - or it arrived with no
/// publication, in which case there is no reading, no fence and no claim to check
/// it against. Neither writing it nor
/// discarding on it is defensible. Writing it lets a cross-attachment replay land
/// a completion nothing can vouch for - the interleaving is ordinary: a consumer
/// holds an unacknowledged page, a replacement receives the marker, the detach
/// beats the replacement's acknowledgement, and on reattach that acknowledgement
/// flushes through the receipt fallback before the orchestrator has even read
/// `get_backfill`, so the fresh walk answers `Skip` and the stranded page is
/// never re-offered. Discarding on it is just as wrong in the other direction: it
/// would delete a marker an earlier attachment legitimately earned. So the answer
/// is to WITHHOLD and touch nothing. A marker that is already durable keeps its
/// row; one that never was has to be earned again by the attachment that can
/// actually judge it.
impl CompletionRefusal {
    fn reason(self) -> &'static str {
        match self {
            Self::OpenDebt => "the scope has open, unwaived debt",
            Self::WalkNotWhole => {
                "the walk that produced this marker did not reach a consumer with every page"
            }
            Self::Unvouchable => {
                "this ledger cannot judge the walk behind the marker: no publication, or one \
                 minted by a prior attachment"
            }
        }
    }
}

/// Persist one acknowledged checkpoint together with everything it proves.
///
/// Three things happen here and they are one indivisible decision:
///
/// 1. The publication's coverage claim is resolved and folded into the ledger.
///    A claim is applied when the CONSUMER acknowledges, never when the account
///    emitted it - an unacknowledged report may describe entries the consumer
///    never persisted, so it cannot be allowed to prove anything.
/// 2. If the checkpoint is a backfill completion sentinel, eligibility is
///    evaluated against the ledger AS IT NOW STANDS. The runner's earlier
///    `complete` flag is not authoritative: a sibling partition's debt may have
///    been accepted since, and writing the sentinel over it makes the next
///    attach skip a scope with open obligations.
/// 3. The checkpoint and the resulting ledger land in ONE store operation.
///
/// The outcome distinguishes "the checkpoint is durable" from "only the ledger
/// was written because the completion sentinel was withheld". The caller must
/// not settle the watermark or announce a durable boundary for the latter.
pub(super) async fn persist_ack_request(
    account_id: &AccountId,
    store: &Arc<DynCheckpointStore>,
    coverage: &PendingCoverage,
    ledger: &mut crate::cursor::DebtLedger,
    req: &AckRequest,
    completed_backfills: &mut HashSet<CursorScope>,
) -> Result<AckPersistOutcome, AckFailure> {
    if let Checkpoint::Change(cursor) = &req.checkpoint {
        cursor
            .validate_envelope()
            .map_err(|_| AckFailure::Rejected(Error::AckRejected(AckRejection::InvalidEnvelope)))?;
    }
    // A BACKFILL acknowledgement whose publication this ledger did not mint is
    // withheld, and checked before the claim lookup so nothing of a prior
    // attachment's evidence is ingested on the way past.
    //
    // The receipt travels with the id the consumer persisted, so an
    // acknowledgement can replay across a detach - re-delivery rather than loss
    // for a change cursor, which simply re-reads from an older position. A
    // backfill row is not a position to re-read from but the position the next
    // WALK starts at: replaying one that a discard removed hands the fresh walk a
    // place to begin beyond a hole, after which an `OpenPages` plan takes its
    // empty end probe, lands a marker, and never offers the stranded window
    // again. The window is ordinary, not exotic: the orchestrator parks on the
    // subscriber gate at attach, which is exactly when a returning consumer
    // subscribes and flushes what it never got to acknowledge - before
    // `get_backfill` has been read.
    //
    // Answered `Ok` with nothing durable, as `Unvouchable` is: the consumer did
    // receive that batch, and the engine has nothing to reproach it with.
    // Known imprecision, accepted rather than open: `minted_here` is false for
    // an id minted by ANOTHER ACCOUNT's ledger in the same process just as it
    // is for a prior attachment of THIS account, so such an id is also
    // answered `Ok` and withheld rather than `Unknown`/`Rejected`. The
    // instance segment is per-`PendingCoverage`, not per-account, so the two
    // cases are indistinguishable here. Withholding is the conservative answer
    // for both - a foreign row is exactly what must not become this account's
    // resume position - so the imprecision costs nothing beyond a slightly
    // generous status. Distinguishing them means putting an account identity
    // into the token. The public token currently binds checkpoint and lane,
    // while the caller still selects the account.
    //
    // Only when the receipt really names THIS batch's lane. A publication from
    // another lane presented with a backfill checkpoint is evidence about the
    // caller, not a replay, and keeps its `Rejected` answer below.
    if let Checkpoint::Backfill(acked) = &req.checkpoint
        && req.publication.as_ref().is_some_and(|id| {
            !coverage.minted_here(id)
                && matches!(
                    id.1.checkpoint.as_ref(),
                    Some(Checkpoint::Backfill(saved))
                        if saved.scope == acked.scope && saved.partition == acked.partition
                )
        })
    {
        tracing::warn!(
            target: "bifrost.sync.backfill",
            account = ?account_id,
            scope = ?req.scope,
            "withholding a backfill row acknowledged from a prior attachment's \
             publication; the next walk must not resume from a position this \
             attachment cannot vouch for"
        );
        persist_ledger_only(account_id, store, ledger)
            .await
            .map_err(AckFailure::Store)?;
        return Ok(AckPersistOutcome::Withheld(CompletionRefusal::Unvouchable));
    }
    let mut candidate = ledger.stage();
    match req
        .publication
        .clone()
        .map(|id| coverage.claim_checkpoint(id, &req.checkpoint))
    {
        Some(ClaimLookup::Apply(claim)) => {
            let now = jiff::Timestamp::now().as_second();
            for report in &claim.reports {
                // Enterprise users expect a failed save to retry owed work,
                // never to report coverage progress through an unrelated save.
                // Keep debt sticky, but apply proof only to the candidate.
                ledger
                    .accrue()
                    .ingest_debt_only(report, claim.generation, now);
            }
            for report in &claim.reports {
                candidate.ingest(report, claim.generation, now);
            }
        }
        Some(ClaimLookup::Replay(reports)) => {
            let now = jiff::Timestamp::now().as_second();
            for report in &reports {
                ledger.accrue().replay_ack_debt(report, now);
                candidate.replay_ack_report(report, now, true);
            }
        }
        // Already persisted by an earlier acknowledgement of this exact
        // publication. Re-applying would double-ingest; reporting failure would
        // make a successful ack non-idempotent. This is still Durable: the
        // caller must promote a provisional change row on this path too, so a
        // delayed reattach abort cannot delete an acknowledged cursor.
        Some(ClaimLookup::AlreadyPersisted) => return Ok(AckPersistOutcome::Durable),
        Some(ClaimLookup::Unknown) => {
            // Emphatically NOT treated as complete coverage. An unknown
            // publication is a stale or buggy caller, and inventing a
            // completeness claim for it is the lying record this whole
            // mechanism exists to prevent.
            return Err(AckFailure::Rejected(Error::AckRejected(
                AckRejection::UnknownPublication,
            )));
        }
        // An engine-internal ack with no coverage claim: leave the ledger
        // exactly as it is.
        None => {}
    }

    if let Checkpoint::Backfill(b) = &req.checkpoint
        && crate::backfill::marker::BackfillKey::decode(&b.partition).is_completion()
        && let Some(reason) = completion_refusal(coverage, &candidate, b, req.publication.as_ref())
    {
        tracing::warn!(
            target: "bifrost.sync.backfill",
            account = ?account_id,
            scope = ?b.scope,
            reason = reason.reason(),
            "withholding backfill completion sentinel"
        );
        // Not an error for the consumer - its batch was empty and its
        // acknowledgement was honoured. The sentinel simply does not become
        // durable, so the next attach re-walks instead of skipping the scope.
        if reason == CompletionRefusal::WalkNotWhole {
            // Deliberately discard the candidate's proof as well as its resume
            // position. A terminal cumulative report from an unwhole walk cannot
            // certify that the consumer received the ground it claims covered.
            // Retain accrued debt in the live ledger: an Outlook user expects
            // missing pages to be offered again, never healed by an empty marker.
            // The rows this walk left behind are a resume position PAST a hole,
            // so they go, and the attempt that wrote them is fenced with them.
            // Withholding the marker alone would leave an `OpenPages` re-attach
            // resuming beyond the pages nobody received - the marker is the only
            // artefact the withholding touched, and it was never the one that
            // carried the position.
            // Bounded by the refused marker's own id. The marker is the last
            // thing its walk published, so everything of that attempt sits at or
            // below it - and a retry that has already started publishing sits
            // above it and is left alone.
            discard_backfill(
                account_id,
                store,
                coverage,
                ledger,
                &b.scope,
                req.publication.as_ref(),
                completed_backfills,
            )
            .await
            .map_err(AckFailure::Store)?;
            return Ok(AckPersistOutcome::Withheld(reason));
        }
        persist_ledger_only(account_id, store, &candidate)
            .await
            .map_err(AckFailure::Store)?;
        candidate.commit(ledger);
        return Ok(AckPersistOutcome::Withheld(reason));
    }

    store
        .apply_transition(
            account_id,
            CheckpointTransition {
                checkpoint: req.checkpoint.clone(),
                ledger: (*candidate).clone(),
            },
        )
        .await
        .map_err(AckFailure::Store)?;
    candidate.commit(ledger);
    Ok(AckPersistOutcome::Durable)
}
