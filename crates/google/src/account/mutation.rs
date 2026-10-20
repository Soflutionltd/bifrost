//! Gmail bulk mutation driver.
//!
//! The driver translates the operation once per stream, posts batches
//! against `users.messages.batchModify` / `batchDelete`, and emits
//! per-id `ItemOutcome<MutationSuccess>` lanes for transmitted batches.
//! Errors funnel through `account_error::into_account_error`; the driver
//! never reaches for `RecoveryClass` directly.
//!
//! The trait signature is `AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>>`.
//! Per-batch outcomes use the `MutationApply` internal enum before being
//! lifted into `SyncEvent` at the stream boundary.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

use bifrost_types::{
    AccountError, AccountOperation, AccountStream, Batch, BatchFailure, BatchItemId, BatchSuccess,
    BatchUncertain, ContainerId, FlagOp, IdempotencyKey, ItemOutcome, LabelId, MembershipScope,
    MutationEffect, MutationSuccess, ObjectId, PageBoundary, SyncEvent, Warning, WarningKind,
};
use futures::{StreamExt, stream};
use serde::Serialize;
use tokio_util::sync::CancellationToken;

use crate::client::GmailClient;
use crate::error::Error as GmailError;

use super::capabilities::{GMAIL_BATCH_MODIFY_LIMIT, GMAIL_MODIFY_LABEL_LIMIT};
use super::error as account_error;
use super::error::{
    GmailErrorContext, is_batch_delete_scope_failure, merge_delete_fallback_error, mutation_error,
    skipped_outcomes,
};
use super::flags;
use super::flags::{LABEL_TRASH, LabelPatch, translate_flag_op};
use super::scopes::{ScopeCache, labels_for_flags};

#[cfg(test)]
pub(crate) fn bulk_set_flags(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    op: FlagOp,
    key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    bulk_set_flags_cancellable(client, cache, targets, op, key, CancellationToken::new())
}

pub(crate) fn bulk_set_flags_cancellable(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    op: FlagOp,
    key: IdempotencyKey,
    shutdown: CancellationToken,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    if let Err(error) = op.validate_for_account(bifrost_types::Protocol::Gmail) {
        return Box::pin(stream::once(async move { SyncEvent::Terminated(error) }));
    }
    mutation_stream(
        client,
        cache,
        targets,
        MutationKind::SetFlags(op),
        key,
        shutdown,
    )
}

#[cfg(test)]
pub(crate) fn bulk_move(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    destination: MembershipScope,
    source: Option<MembershipScope>,
    key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    bulk_move_cancellable(
        client,
        cache,
        targets,
        destination,
        source,
        key,
        CancellationToken::new(),
    )
}

pub(crate) fn bulk_move_cancellable(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    destination: MembershipScope,
    source: Option<MembershipScope>,
    key: IdempotencyKey,
    shutdown: CancellationToken,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    mutation_stream(
        client,
        cache,
        targets,
        MutationKind::Move {
            destination,
            source,
        },
        key,
        shutdown,
    )
}

#[cfg(test)]
pub(crate) fn bulk_destroy(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    key: IdempotencyKey,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    bulk_destroy_cancellable(client, cache, targets, key, CancellationToken::new())
}

pub(crate) fn bulk_destroy_cancellable(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    key: IdempotencyKey,
    shutdown: CancellationToken,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    mutation_stream(client, cache, targets, MutationKind::Destroy, key, shutdown)
}

fn mutation_stream(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    kind: MutationKind,
    key: IdempotencyKey,
    shutdown: CancellationToken,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    // One batch here is one `batchModify` / `batchDelete` call plus any
    // label refresh and any per-id TRASH fallback the driver had to
    // fall back on, so the accumulator is what makes those fallback
    // requests visible rather than free.
    let (client, tally) = client.metered();
    let client = Arc::new(client);
    let state = MutationState {
        client,
        tally,
        cache,
        targets,
        kind,
        key,
        patch: None,
        pending_target: None,
        finished: false,
        emitted_done: false,
        pending_event: None,
        shutdown,
    };

    Box::pin(stream::unfold(state, |mut state| async move {
        // The pending event is drained BEFORE the shutdown check.
        // Cancellation of a dispatched batch parks a `Terminated` here
        // behind the uncertain lanes; testing the token first would
        // swallow it and turn the very evidence this stream owes the
        // engine back into a silent disappearance.
        if let Some(event) = state.pending_event.take() {
            return Some((event, state));
        }
        if state.shutdown.is_cancelled() {
            return None;
        }
        if state.finished {
            if state.emitted_done {
                return None;
            }
            state.emitted_done = true;
            return Some((SyncEvent::Done(None), state));
        }

        if state.patch.is_none() {
            match &state.kind {
                MutationKind::SetFlags(op) => {
                    let labels_result = tokio::select! {
                        () = state.shutdown.cancelled() => return None,
                        result = labels_for_flags(&state.client, &state.cache) => result,
                    };
                    let labels = match labels_result {
                        Ok(labels) => labels,
                        Err(error) => {
                            state.finished = true;
                            state.emitted_done = true;
                            let account_error = account_error::into_account_error(
                                error,
                                GmailErrorContext::mutation(AccountOperation::UpdateFlags),
                            );
                            return Some((SyncEvent::Terminated(account_error), state));
                        }
                    };
                    state.patch = Some(translate_flag_op(op, &labels));
                }
                MutationKind::Move {
                    destination,
                    source,
                } => {
                    state.patch = Some(move_patch(destination, source.as_ref()));
                }
                MutationKind::Destroy => {}
            }
        }

        let mut ids = Vec::new();
        if let Some(id) = state.pending_target.take() {
            ids.push(id);
        }
        let mut targets_exhausted = false;
        while ids.len() < GMAIL_BATCH_MODIFY_LIMIT {
            let next = tokio::select! {
                () = state.shutdown.cancelled() => return None,
                next = state.targets.next() => next,
            };
            match next {
                Some(id) => ids.push(id),
                None => {
                    targets_exhausted = true;
                    break;
                }
            }
        }
        if !targets_exhausted && ids.len() == GMAIL_BATCH_MODIFY_LIMIT {
            let next = tokio::select! {
                () = state.shutdown.cancelled() => return None,
                next = state.targets.next() => next,
            };
            match next {
                Some(id) => state.pending_target = Some(id),
                None => targets_exhausted = true,
            }
        }
        if ids.is_empty() {
            state.finished = true;
            state.emitted_done = true;
            return Some((SyncEvent::Done(None), state));
        }

        let operation = state.kind.operation();
        let started = Instant::now();
        // Last preemption point that costs nothing: no byte has crossed
        // the side-effect boundary for this batch yet, so ending here
        // reports nothing and loses nothing.
        if state.shutdown.is_cancelled() {
            return None;
        }
        let body_count = match &state.kind {
            MutationKind::SetFlags(_) | MutationKind::Move { .. } => {
                let patch = state.patch.clone().unwrap_or_default();
                batch_modify_bodies(&ids, &patch).len()
            }
            MutationKind::Destroy => 1,
        };
        let mutation_progress = Arc::new(Mutex::new(MutationProgress::new(&ids, body_count)));
        let progress_for_operation = Arc::clone(&mutation_progress);
        let operation_future = async {
            match &state.kind {
                MutationKind::Destroy => {
                    apply_destroy(
                        &state.client,
                        &ids,
                        &state.key,
                        Arc::clone(&progress_for_operation),
                    )
                    .await
                }
                MutationKind::SetFlags(_) | MutationKind::Move { .. } => {
                    let patch = state.patch.clone().unwrap_or_default();
                    apply_label_patch_tracked(
                        &state.client,
                        &ids,
                        patch,
                        &state.key,
                        operation,
                        Arc::clone(&progress_for_operation),
                    )
                    .await
                }
            }
        };
        let event = tokio::select! {
            // Biased so a request that has already answered is
            // classified normally even when the token fires in the same
            // poll; cancellation must never discard a completed answer.
            biased;
            event = operation_future => event,
            () = state.shutdown.cancelled() => {
                // Preserve completed outcomes, then mark only ids with a
                // partial write or an unanswered request as uncertain.
                // Unattempted ids have no lane yet and remain for the
                // engine to re-issue after this stream terminates.
                let error = account_error::shutdown_inflight_error(operation);
                let accounted = mutation_progress
                    .lock()
                    .expect("mutation progress lock poisoned")
                    .shutdown_outcomes(&error);
                MutationApply::Terminate {
                    accounted,
                    error,
                }
            }
        };

        match event {
            MutationApply::Batch { items, warning } => {
                if targets_exhausted {
                    state.finished = true;
                }
                let batch = SyncEvent::Batch(Batch {
                    items,
                    page_boundary: if targets_exhausted {
                        PageBoundary::Final
                    } else {
                        PageBoundary::Page
                    },
                    server_latency: started.elapsed(),
                    bytes_in: state.tally.take(),
                    checkpoint: None,
                });
                if let Some(warning) = warning {
                    state.pending_event = Some(batch);
                    Some((SyncEvent::Warning(warning), state))
                } else {
                    Some((batch, state))
                }
            }
            MutationApply::Terminate { accounted, error } => {
                state.finished = true;
                state.emitted_done = true;
                if accounted.is_empty() {
                    return Some((terminate_event(error), state));
                }
                // Ids the bisection already resolved get their lane
                // before the stream ends. The boundary is `Page`, never
                // `Final`: the operation did not complete, and claiming
                // a final page would tell the engine the remaining
                // targets were considered.
                state.pending_event = Some(terminate_event(error));
                Some((
                    SyncEvent::Batch(Batch {
                        items: accounted,
                        page_boundary: PageBoundary::Page,
                        server_latency: started.elapsed(),
                        bytes_in: state.tally.take(),
                        checkpoint: None,
                    }),
                    state,
                ))
            }
        }
    }))
}

enum MutationKind {
    SetFlags(FlagOp),
    Move {
        destination: MembershipScope,
        source: Option<MembershipScope>,
    },
    Destroy,
}

impl MutationKind {
    fn operation(&self) -> AccountOperation {
        match self {
            Self::SetFlags(_) => AccountOperation::UpdateFlags,
            Self::Move { .. } => AccountOperation::BulkMove,
            Self::Destroy => AccountOperation::BulkDestroy,
        }
    }
}

struct MutationState {
    client: Arc<GmailClient>,
    tally: crate::client::ByteTally,
    cache: ScopeCache,
    targets: AccountStream<ObjectId>,
    kind: MutationKind,
    key: IdempotencyKey,
    patch: Option<LabelPatch>,
    pending_target: Option<ObjectId>,
    finished: bool,
    emitted_done: bool,
    pending_event: Option<SyncEvent<ItemOutcome<MutationSuccess>>>,
    shutdown: CancellationToken,
}

enum MutationApply {
    Batch {
        items: Vec<ItemOutcome<MutationSuccess>>,
        warning: Option<Warning>,
    },
    /// The stream must end. `accounted` carries the outcomes of ids the
    /// driver has resolved or partially applied before the terminal error
    /// arrived. These ids are emitted as one batch ahead of `Terminated`;
    /// ids with no acknowledged effect and no ambiguous request stay
    /// unreported so the engine can re-issue them.
    Terminate {
        accounted: Vec<ItemOutcome<MutationSuccess>>,
        error: AccountError,
    },
}

/// Evidence for one mutation batch across its requests.
///
/// A label body may be sent several times while its ids are bisected after a
/// 404, so each id counts a body at most once. Finished outcomes are retained
/// if a later request terminates or shutdown drops the operation future.
struct MutationProgress {
    ids: Vec<ObjectId>,
    positions: HashMap<String, Vec<usize>>,
    body_count: usize,
    completed_effect: Option<MutationEffect>,
    states: Vec<MutationIdProgress>,
}

enum MutationIdProgress {
    Active {
        acknowledged_bodies: usize,
        last_acknowledged_body: Option<usize>,
        in_flight: bool,
    },
    Finished(ItemOutcome<MutationSuccess>),
}

impl MutationProgress {
    fn new(ids: &[ObjectId], body_count: usize) -> Self {
        let mut positions = HashMap::<String, Vec<usize>>::new();
        for (index, id) in ids.iter().enumerate() {
            positions.entry(id.0.clone()).or_default().push(index);
        }
        Self {
            ids: ids.to_vec(),
            positions,
            body_count,
            completed_effect: None,
            states: ids
                .iter()
                .map(|_| MutationIdProgress::Active {
                    acknowledged_bodies: 0,
                    last_acknowledged_body: None,
                    in_flight: false,
                })
                .collect(),
        }
    }

    fn begin(&mut self, ids: &[ObjectId]) {
        for id in ids {
            if let Some(indices) = self.positions.get(&id.0) {
                for &index in indices {
                    if let MutationIdProgress::Active { in_flight, .. } = &mut self.states[index] {
                        *in_flight = true;
                    }
                }
            }
        }
    }

    fn end_request(&mut self, ids: &[ObjectId]) {
        for id in ids {
            if let Some(indices) = self.positions.get(&id.0) {
                for &index in indices {
                    if let MutationIdProgress::Active { in_flight, .. } = &mut self.states[index] {
                        *in_flight = false;
                    }
                }
            }
        }
    }

    fn acknowledge(&mut self, ids: &[ObjectId], body_index: usize, unsupported: &[String]) {
        for id in ids {
            let Some(indices) = self.positions.get(&id.0) else {
                continue;
            };
            for &index in indices {
                let complete = match &mut self.states[index] {
                    MutationIdProgress::Active {
                        acknowledged_bodies,
                        last_acknowledged_body,
                        in_flight,
                    } => {
                        *in_flight = false;
                        if *last_acknowledged_body != Some(body_index) {
                            *acknowledged_bodies += 1;
                            *last_acknowledged_body = Some(body_index);
                        }
                        *acknowledged_bodies == self.body_count
                    }
                    MutationIdProgress::Finished(_) => false,
                };
                if complete {
                    let output = if let Some(actual) = &self.completed_effect {
                        MutationSuccess::Downgraded {
                            actual: actual.clone(),
                        }
                    } else if unsupported.is_empty() {
                        MutationSuccess::Applied
                    } else {
                        MutationSuccess::Downgraded {
                            actual: MutationEffect::FlagsPartiallyApplied {
                                unsupported: unsupported.to_vec(),
                            },
                        }
                    };
                    self.states[index] = MutationIdProgress::Finished(ItemOutcome::Succeeded(
                        BatchSuccess::new(BatchItemId(self.ids[index].0.clone()), output),
                    ));
                }
            }
        }
    }

    fn finish_failures(&mut self, outcomes: Vec<ItemOutcome<MutationSuccess>>, not_found: bool) {
        for outcome in outcomes {
            let ItemOutcome::Failed(failure) = outcome else {
                continue;
            };
            let Some(indices) = self.positions.get(&failure.item.0) else {
                continue;
            };
            for &index in indices {
                let has_partial_effect = matches!(
                    &self.states[index],
                    MutationIdProgress::Active {
                        acknowledged_bodies,
                        ..
                    } if *acknowledged_bodies > 0
                );
                // A permanent non-404 refusal cannot erase an earlier
                // acknowledged body. `Failed` would claim no effect, so keep
                // the error but route the id through read-back as uncertain.
                let outcome = if has_partial_effect && !not_found {
                    ItemOutcome::Uncertain(BatchUncertain::new(
                        failure.item.clone(),
                        failure.error.clone(),
                    ))
                } else {
                    ItemOutcome::Failed(BatchFailure::new(
                        failure.item.clone(),
                        failure.error.clone(),
                    ))
                };
                if matches!(&self.states[index], MutationIdProgress::Active { .. }) {
                    self.states[index] = MutationIdProgress::Finished(outcome);
                }
            }
        }
    }

    fn completed_outcomes(&self) -> Vec<ItemOutcome<MutationSuccess>> {
        self.states
            .iter()
            .filter_map(|state| match state {
                MutationIdProgress::Finished(outcome) => Some(outcome.clone()),
                MutationIdProgress::Active { .. } => None,
            })
            .collect()
    }

    fn terminal_outcomes(&self, error: &AccountError) -> Vec<ItemOutcome<MutationSuccess>> {
        self.outcomes_before_stop(error, false)
    }

    fn shutdown_outcomes(&self, error: &AccountError) -> Vec<ItemOutcome<MutationSuccess>> {
        self.outcomes_before_stop(error, true)
    }

    fn outcomes_before_stop(
        &self,
        error: &AccountError,
        cancellation: bool,
    ) -> Vec<ItemOutcome<MutationSuccess>> {
        let ambiguous_attempt = error.chain().iter().any(|cause| {
            matches!(
                cause,
                bifrost_types::Cause::Attempt(attempt)
                    if attempt.transmission_state == bifrost_types::TransmissionState::InFlight
            )
        });
        self.ids
            .iter()
            .zip(&self.states)
            .filter_map(|(id, state)| match state {
                MutationIdProgress::Finished(outcome) => Some(outcome.clone()),
                MutationIdProgress::Active {
                    acknowledged_bodies,
                    in_flight,
                    ..
                } if *acknowledged_bodies > 0
                    || (*in_flight && (cancellation || ambiguous_attempt)) =>
                {
                    Some(ItemOutcome::Uncertain(BatchUncertain::new(
                        BatchItemId(id.0.clone()),
                        error.clone(),
                    )))
                }
                MutationIdProgress::Active { .. } => None,
            })
            .collect()
    }
}

async fn apply_label_patch_tracked(
    client: &GmailClient,
    ids: &[ObjectId],
    patch: LabelPatch,
    key: &IdempotencyKey,
    operation: AccountOperation,
    progress: Arc<Mutex<MutationProgress>>,
) -> MutationApply {
    if operation == AccountOperation::BulkMove && !patch.unsupported_flags.is_empty() {
        let detail = format!(
            "unsupported Gmail mutation scope: {}",
            patch.unsupported_flags.join(", ")
        );
        let error = account_error::into_account_error(
            GmailError::invalid_request(operation, detail),
            GmailErrorContext::mutation(operation),
        );
        return MutationApply::Batch {
            items: failed_outcomes(ids, &error),
            warning: None,
        };
    }
    if patch.add_label_ids.is_empty() && patch.remove_label_ids.is_empty() {
        if patch.unsupported_flags.is_empty() {
            return MutationApply::Batch {
                items: skipped_outcomes(ids),
                warning: None,
            };
        }
        // Nothing Gmail can express, so no request goes out and nothing
        // changes. This is NOT a success of any flavour: `Skipped` claims
        // the target was already in the requested state, and `Downgraded`
        // claims the target changed into a weaker state - both are false
        // here, and both are contract-load-bearing for the other protocol
        // crates and for the engine's read-back guard. The honest lane is
        // a per-id failure classified `Unsupported`, which the recovery
        // table maps to a permanent no-retry outcome rather than to a
        // ClientBug that would poison the rest of the operation.
        let error = account_error::into_account_error(
            GmailError::unsupported_with(
                operation,
                format!(
                    "Gmail cannot represent any requested flag: {}",
                    patch.unsupported_flags.join(", ")
                ),
            ),
            GmailErrorContext::mutation(operation),
        );
        return MutationApply::Batch {
            items: failed_outcomes(ids, &error),
            warning: Some(unsupported_flags_warning(&patch.unsupported_flags)),
        };
    }
    let mut live_ids = ids.to_vec();
    for (body_index, body) in batch_modify_bodies(ids, &patch).into_iter().enumerate() {
        if live_ids.is_empty() {
            break;
        }
        match apply_label_patch_body(
            client,
            &live_ids,
            &body,
            key,
            LabelPatchBodyContext {
                operation,
                unsupported: &patch.unsupported_flags,
                body_index,
                progress: &progress,
            },
        )
        .await
        {
            Ok(next_live_ids) => live_ids = next_live_ids,
            Err(error) => {
                let accounted = progress
                    .lock()
                    .expect("mutation progress lock poisoned")
                    .terminal_outcomes(&error);
                return MutationApply::Terminate { accounted, error };
            }
        }
    }
    MutationApply::Batch {
        items: progress
            .lock()
            .expect("mutation progress lock poisoned")
            .completed_outcomes(),
        warning: (!patch.unsupported_flags.is_empty())
            .then(|| unsupported_flags_warning(&patch.unsupported_flags)),
    }
}

/// Apply one label body, bisecting its ids on 404 and returning only the ids
/// that succeeded so the caller can carry them into the next label body.
struct LabelPatchBodyContext<'a> {
    operation: AccountOperation,
    unsupported: &'a [String],
    body_index: usize,
    progress: &'a Arc<Mutex<MutationProgress>>,
}

async fn apply_label_patch_body(
    client: &GmailClient,
    ids: &[ObjectId],
    template: &BatchModifyRequest,
    key: &IdempotencyKey,
    context: LabelPatchBodyContext<'_>,
) -> Result<Vec<ObjectId>, AccountError> {
    let LabelPatchBodyContext {
        operation,
        unsupported,
        body_index,
        progress,
    } = context;
    if ids.is_empty() {
        return Ok(Vec::new());
    }
    let mut pending = vec![ids.to_vec()];
    let mut live_ids = Vec::new();
    while let Some(part) = pending.pop() {
        let body = BatchModifyRequest {
            ids: part.iter().map(|id| id.0.clone()).collect(),
            add_label_ids: template.add_label_ids.clone(),
            remove_label_ids: template.remove_label_ids.clone(),
        };
        progress
            .lock()
            .expect("mutation progress lock poisoned")
            .begin(&part);
        match post_empty_json(client, "/messages/batchModify", &body, key).await {
            Ok(()) => {
                progress
                    .lock()
                    .expect("mutation progress lock poisoned")
                    .acknowledge(&part, body_index, unsupported);
                live_ids.extend(part);
            }
            Err(error) if is_not_found(&error) && part.len() > 1 => {
                progress
                    .lock()
                    .expect("mutation progress lock poisoned")
                    .end_request(&part);
                let midpoint = part.len() / 2;
                pending.push(part[midpoint..].to_vec());
                pending.push(part[..midpoint].to_vec());
            }
            Err(error) => {
                let not_found = is_not_found(&error);
                match mutation_error(&part, error, GmailErrorContext::mutation(operation)) {
                    Ok(outcomes) => {
                        let mut progress =
                            progress.lock().expect("mutation progress lock poisoned");
                        progress.end_request(&part);
                        progress.finish_failures(outcomes, not_found);
                    }
                    Err(error) => return Err(error),
                }
            }
        }
    }
    Ok(live_ids)
}

/// Lower one `LabelPatch` onto the `batchModify` request bodies that carry
/// it, splitting the label lists at `GMAIL_MODIFY_LABEL_LIMIT`.
///
/// `GMAIL_BATCH_MODIFY_LIMIT` bounds the ids, and the caller has already
/// chunked against it. Nothing bounds the LABEL lists on the way in:
/// `flags::patch_for_set` names every user label in the account in
/// `removeLabelIds` by construction, so an account with more than a
/// hundred labels overruns Gmail's documented per-update label cap on
/// every exact-set. The 404 recovery bisects only the id list, which does
/// not shrink the label list at all, so the label split has to happen
/// here instead.
///
/// Splitting is sound because a label patch is state-independent and
/// idempotent: each request asserts an absolute add-or-remove for labels
/// it names and says nothing about the rest. Applying every body to each
/// live id therefore converges on the same final label set as one
/// oversized request would have. A 404 can narrow the live id list for
/// the remaining bodies, but no later label chunk is skipped for an id
/// that survived the failed body's bisection. Only intermediate states
/// are observable, and Gmail offers no atomicity across ids in one call
/// either. The single caveat is a caller CONTRADICTION - the same label in
/// both lists, pinned in `flags` as removal-wins - whose resolution is
/// per-request, so a contradiction split across bodies resolves by last
/// write rather than by removal. That patch is malformed on the way in;
/// this does not make it more so.
///
/// Common accounts stay far under the cap and get exactly one body, i.e.
/// the pre-chunking request, unchanged.
fn batch_modify_bodies(ids: &[ObjectId], patch: &LabelPatch) -> Vec<BatchModifyRequest> {
    let message_ids: Vec<String> = ids.iter().map(|id| id.0.clone()).collect();
    let mut adds = patch.add_label_ids.chunks(GMAIL_MODIFY_LABEL_LIMIT);
    let mut removes = patch.remove_label_ids.chunks(GMAIL_MODIFY_LABEL_LIMIT);
    let mut bodies = Vec::new();
    loop {
        let (add, remove) = (adds.next(), removes.next());
        if add.is_none() && remove.is_none() {
            break;
        }
        bodies.push(BatchModifyRequest {
            ids: message_ids.clone(),
            add_label_ids: add.unwrap_or_default().to_vec(),
            remove_label_ids: remove.unwrap_or_default().to_vec(),
        });
    }
    bodies
}

fn is_not_found(error: &GmailError) -> bool {
    // Keep the bisection predicate narrow. Gmail's batchModify contract
    // exposes message ids only as strings and publishes no syntax grammar or
    // per-id detail for `invalidArgument`; a 400 can also describe a shared
    // label body. Splitting every 400 could turn one invalid patch into up to
    // roughly 2n requests without establishing which id was malformed. Until
    // Gmail gives item-specific evidence or documents a local id grammar, a
    // generic 400 stays a batch failure.
    account_error::response_parts(error).is_some_and(|(status, _)| status == 404)
}

fn failed_outcomes(ids: &[ObjectId], error: &AccountError) -> Vec<ItemOutcome<MutationSuccess>> {
    ids.iter()
        .map(|id| ItemOutcome::Failed(BatchFailure::new(BatchItemId(id.0.clone()), error.clone())))
        .collect()
}

fn unsupported_flags_warning(unsupported: &[String]) -> Warning {
    Warning::support_only(
        WarningKind::StrategyDowngraded,
        format!("Gmail could not apply flags: {}", unsupported.join(", ")),
    )
}

async fn apply_destroy(
    client: &GmailClient,
    ids: &[ObjectId],
    key: &IdempotencyKey,
    progress: Arc<Mutex<MutationProgress>>,
) -> MutationApply {
    let body = BatchDeleteRequest {
        ids: ids.iter().map(|id| id.0.clone()).collect(),
    };
    progress
        .lock()
        .expect("mutation progress lock poisoned")
        .begin(ids);
    match post_empty_json(client, "/messages/batchDelete", &body, key).await {
        Ok(()) => {
            progress
                .lock()
                .expect("mutation progress lock poisoned")
                .acknowledge(ids, 0, &[]);
            MutationApply::Batch {
                items: progress
                    .lock()
                    .expect("mutation progress lock poisoned")
                    .completed_outcomes(),
                warning: None,
            }
        }
        Err(error) if is_batch_delete_scope_failure(&error) => {
            progress
                .lock()
                .expect("mutation progress lock poisoned")
                .end_request(ids);
            // Translate the primary failure once and consume
            // it. The original `Error` is not used after this point;
            // the fallback diagnostic attaches the primary's outermost
            // cause via `merge_delete_fallback_error`.
            let primary = account_error::into_account_error(
                error,
                GmailErrorContext::mutation(AccountOperation::BulkDestroy),
            );
            // The fallback is a move into TRASH, so it goes through the
            // same relocation rule as every other move rather than
            // hand-rolling a patch that would leave SPAM attached.
            // Record the actual effect when each sub-batch is acknowledged.
            // Shutdown reads this ledger without waiting for this future to return.
            progress
                .lock()
                .expect("mutation progress lock poisoned")
                .completed_effect = Some(MutationEffect::MovedToContainer(ContainerId(
                LABEL_TRASH.to_string(),
            )));
            let fallback = flags::move_placement_patch(LABEL_TRASH);
            match apply_label_patch_tracked(
                client,
                ids,
                fallback,
                key,
                AccountOperation::BulkDestroy,
                Arc::clone(&progress),
            )
            .await
            {
                MutationApply::Batch { items, warning } => MutationApply::Batch { items, warning },
                MutationApply::Terminate {
                    accounted,
                    error: fallback_error,
                } => MutationApply::Terminate {
                    accounted,
                    error: merge_delete_fallback_error(fallback_error, &primary),
                },
            }
        }
        Err(error) => {
            let not_found = is_not_found(&error);
            match mutation_error(
                ids,
                error,
                GmailErrorContext::mutation(AccountOperation::BulkDestroy),
            ) {
                Ok(outcomes) => {
                    let mut progress = progress.lock().expect("mutation progress lock poisoned");
                    progress.end_request(ids);
                    progress.finish_failures(outcomes, not_found);
                    MutationApply::Batch {
                        items: progress.completed_outcomes(),
                        warning: None,
                    }
                }
                Err(error) => {
                    let accounted = progress
                        .lock()
                        .expect("mutation progress lock poisoned")
                        .terminal_outcomes(&error);
                    MutationApply::Terminate { accounted, error }
                }
            }
        }
    }
}

fn terminate_event(error: AccountError) -> SyncEvent<ItemOutcome<MutationSuccess>> {
    SyncEvent::Terminated(error)
}

/// Translate a bulk move destination (and optional source) into a
/// Gmail label patch.
///
/// Gmail has no "move" verb; a move is an add plus the removal of every
/// container the message is leaving. The exclusive-container half of
/// that is destination-derived and lives in
/// [`flags::move_placement_patch`], shared with the single-object
/// builders in `pim.rs` so both entry points agree on the wire shape.
///
/// `source` covers the part the destination cannot imply: a *user*
/// label the message is being filed out of. Folding it into the same
/// `batchModify` is what lets a consumer drop the O(n) "bulk_move plus
/// a per-id `remove_from_container`" composition - Gmail's
/// `batchModify` expresses add-and-remove in one request.
fn move_patch(destination: &MembershipScope, source: Option<&MembershipScope>) -> LabelPatch {
    let Some(LabelId(destination_id)) = as_label(destination) else {
        return LabelPatch {
            unsupported_flags: vec!["gmail move destination must be a label".to_string()],
            ..LabelPatch::default()
        };
    };
    let mut patch = flags::move_placement_patch(destination_id);
    match source {
        None => {}
        Some(scope) => {
            let Some(LabelId(source_id)) = as_label(scope) else {
                return LabelPatch {
                    unsupported_flags: vec!["gmail move source must be a label".to_string()],
                    ..LabelPatch::default()
                };
            };
            // Removing the synthetic `archive` id is a no-op (there is
            // no such Gmail label), and a source equal to the
            // destination would ask Gmail to add and remove the same
            // label in one request.
            let redundant = flags::is_archive_id(source_id)
                || source_id.eq_ignore_ascii_case(destination_id)
                || patch
                    .remove_label_ids
                    .iter()
                    .any(|existing| existing.eq_ignore_ascii_case(source_id));
            if !redundant {
                patch.remove_label_ids.push(source_id.clone());
            }
        }
    }
    patch
}

fn as_label(scope: &MembershipScope) -> Option<&LabelId> {
    match scope {
        MembershipScope::Label(label) => Some(label),
        _ => None,
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct BatchModifyRequest {
    ids: Vec<String>,
    add_label_ids: Vec<String>,
    remove_label_ids: Vec<String>,
}

#[derive(Serialize)]
struct BatchDeleteRequest {
    ids: Vec<String>,
}

async fn post_empty_json<B: Serialize>(
    client: &GmailClient,
    path: &str,
    body: &B,
    _key: &IdempotencyKey,
) -> crate::Result<()> {
    // Gmail messages endpoints accept no documented client-mintable
    // replay token, so the Account idempotency key stays engine-side.
    // URL assembly is `GmailClient::api_url`'s job, not this function's.
    // Restating the join here let the raw-builder path drift from the
    // typed one; the absolute-URL case in particular was missing.
    let url = client.api_url(path);
    // Built through `account_net()` rather than `GmailClient::execute`,
    // so the per-method quota cost has to be applied by hand here - the
    // batch endpoints are 50 units each, and billing them as one would
    // hand the batch lane a free ride on the shared per-user budget.
    // `json` supplies the JSON content type.
    let mut request = client.account_net().post(&url).json(body);
    if let Some(cost) = client.gmail_quota_cost(&url, "POST") {
        request = request.cost(cost);
    }
    let response = client.execute_builder(request, "Gmail API").await?;
    let status = response.status();
    if status.is_success() {
        return Ok(());
    }
    let headers = crate::error::GmailResponseHeaders::from_headers(response.headers());
    Err(GmailError::response_from_parts(
        crate::error::GmailService::GmailApi,
        status.as_u16(),
        headers,
        response.body,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use bifrost_net::RetryPolicy;
    use bifrost_net::auth::StaticTokenSource;
    use bifrost_net::test_support::{Canned, ScriptedDispatch};
    use bifrost_net::{NetConfig, test_support};
    use bifrost_types::{FolderId, ProtocolSalt, RunId};
    use bytes::Bytes;
    use reqwest::StatusCode;
    use std::collections::HashSet;
    use std::time::Instant;

    fn canned(status: StatusCode) -> Canned {
        Canned::Response {
            status,
            headers: reqwest::header::HeaderMap::new(),
            body: if status == StatusCode::NOT_FOUND {
                Bytes::from_static(br#"{"error":{"code":404,"status":"NOT_FOUND"}}"#)
            } else {
                Bytes::new()
            },
        }
    }

    /// A batch already on the wire when `close()` fires must not vanish.
    /// Gmail may have applied it, so every consumed id owes the engine an
    /// `Uncertain` lane plus a terminator carrying `InFlight` evidence.
    #[tokio::test(start_paused = true)]
    async fn mutation_cancelled_mid_flight_reports_uncertain_for_every_dispatched_id() {
        let (client, script) = scripted_client(vec![Canned::Pending, Canned::Pending]);
        let shutdown = CancellationToken::new();
        let mut events = bulk_destroy_cancellable(
            client,
            fresh_cache(Vec::new()),
            Box::pin(stream::iter([
                ObjectId("m1".to_string()),
                ObjectId("m2".to_string()),
            ])),
            test_key(99),
            shutdown.clone(),
        );
        let started = tokio::time::Instant::now();
        let collected = tokio::spawn(async move {
            let mut out = Vec::new();
            while let Some(event) = events.next().await {
                out.push(event);
            }
            out
        });
        while script.requests().is_empty() {
            tokio::task::yield_now().await;
        }
        shutdown.cancel();
        let events = tokio::time::timeout(std::time::Duration::from_secs(1), collected)
            .await
            .expect("cancellation must beat the one-second deadline")
            .expect("mutation task joins");
        // Elapsed time and request count are what separate an observed
        // token from a stream that merely ran out of scripted answers.
        assert_eq!(started.elapsed(), std::time::Duration::ZERO);
        assert_eq!(script.requests().len(), 1);

        assert_eq!(events.len(), 2, "expected one batch then a terminator");
        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("expected the uncertain batch first, got {:?}", events[0]);
        };
        assert_eq!(
            batch.page_boundary,
            PageBoundary::Page,
            "an abandoned batch must never claim a final page"
        );
        let uncertain = batch
            .items
            .iter()
            .map(|item| match item {
                ItemOutcome::Uncertain(entry) => entry.item.0.clone(),
                other => panic!("dispatched ids must be Uncertain, got {other:?}"),
            })
            .collect::<Vec<_>>();
        assert_eq!(uncertain, vec!["m1".to_string(), "m2".to_string()]);
        let ItemOutcome::Uncertain(entry) = &batch.items[0] else {
            unreachable!()
        };
        assert!(
            entry.error.chain().iter().any(|cause| matches!(
                cause,
                bifrost_types::Cause::Attempt(attempt)
                    if attempt.transmission_state == bifrost_types::TransmissionState::InFlight
            )),
            "the uncertain lane must carry InFlight transmission evidence"
        );
        assert!(matches!(&events[1], SyncEvent::Terminated(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_bisection_keeps_resolved_lanes_and_marks_in_flight_ids_uncertain() {
        let (client, script) = scripted_client(vec![
            canned(StatusCode::NOT_FOUND),
            canned(StatusCode::NO_CONTENT),
            canned(StatusCode::NOT_FOUND),
            canned(StatusCode::NOT_FOUND),
            Canned::Pending,
        ]);
        let shutdown = CancellationToken::new();
        let mut events = bulk_move_cancellable(
            client,
            fresh_cache(Vec::new()),
            Box::pin(stream::iter(
                ["a", "b", "c", "d"].map(|id| ObjectId(id.to_string())),
            )),
            label("INBOX"),
            None,
            test_key(100),
            shutdown.clone(),
        );
        let collected = tokio::spawn(async move {
            let mut out = Vec::new();
            while let Some(event) = events.next().await {
                out.push(event);
            }
            out
        });
        while script.requests().len() < 5 {
            tokio::task::yield_now().await;
        }
        shutdown.cancel();
        let events = tokio::time::timeout(std::time::Duration::from_secs(1), collected)
            .await
            .expect("cancellation must end the in-flight sub-batch")
            .expect("mutation task joins");

        assert_eq!(events.len(), 2, "expected an accounted page and terminator");
        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("the bisection outcomes must precede termination");
        };
        assert_eq!(batch.page_boundary, PageBoundary::Page);
        assert!(matches!(
            batch.items.as_slice(),
            [
                ItemOutcome::Succeeded(first),
                ItemOutcome::Succeeded(second),
                ItemOutcome::Failed(missing),
                ItemOutcome::Uncertain(in_flight),
            ] if first.item.0 == "a"
                && second.item.0 == "b"
                && missing.item.0 == "c"
                && in_flight.item.0 == "d"
        ));
        let ItemOutcome::Uncertain(in_flight) = &batch.items[3] else {
            unreachable!()
        };
        assert!(in_flight.error.chain().iter().any(|cause| matches!(
            cause,
            bifrost_types::Cause::Attempt(attempt)
                if attempt.transmission_state == bifrost_types::TransmissionState::InFlight
        )));
        assert!(matches!(&events[1], SyncEvent::Terminated(_)));
    }

    #[tokio::test(start_paused = true)]
    async fn cancelled_trash_fallback_preserves_downgraded_sub_batch() {
        let forbidden = Canned::Response {
            status: StatusCode::FORBIDDEN,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from_static(
                br#"{"error":{"code":403,"errors":[{"reason":"forbidden"}]}}"#,
            ),
        };
        let (client, script) = scripted_client(vec![
            forbidden,
            canned(StatusCode::NOT_FOUND),
            canned(StatusCode::NO_CONTENT),
            Canned::Pending,
        ]);
        let shutdown = CancellationToken::new();
        let events = bulk_destroy_cancellable(
            client,
            fresh_cache(Vec::new()),
            Box::pin(stream::iter(
                ["trashed", "pending"].map(|id| ObjectId(id.to_string())),
            )),
            test_key(101),
            shutdown.clone(),
        );
        let collected = tokio::spawn(collect_events(events));
        while script.requests().len() < 4 {
            tokio::task::yield_now().await;
        }
        shutdown.cancel();
        let events = tokio::time::timeout(std::time::Duration::from_secs(1), collected)
            .await
            .expect("shutdown must end the pending trash sub-batch")
            .expect("mutation task joins");

        assert_eq!(script.requests().len(), 4);
        assert_eq!(events.len(), 2, "expected an accounted page and terminator");
        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("trash outcomes must precede termination");
        };
        assert_eq!(batch.page_boundary, PageBoundary::Page);
        assert!(matches!(
            batch.items.as_slice(),
            [ItemOutcome::Succeeded(trashed), ItemOutcome::Uncertain(pending)]
                if trashed.item.0 == "trashed"
                    && trashed.output == MutationSuccess::Downgraded {
                        actual: MutationEffect::MovedToContainer(ContainerId("TRASH".to_string())),
                    }
                    && pending.item.0 == "pending"
        ));
        let ItemOutcome::Uncertain(pending) = &batch.items[1] else {
            unreachable!()
        };
        assert!(pending.error.chain().iter().any(|cause| matches!(
            cause,
            bifrost_types::Cause::Attempt(attempt)
                if attempt.transmission_state == bifrost_types::TransmissionState::InFlight
        )));
        assert!(matches!(&events[1], SyncEvent::Terminated(_)));
    }

    /// The mirror case: cancellation BEFORE any byte crosses the
    /// side-effect boundary owes nothing and reports nothing.
    #[tokio::test(start_paused = true)]
    async fn mutation_cancelled_before_dispatch_reports_nothing() {
        let (client, script) = scripted_client(vec![Canned::Pending]);
        let shutdown = CancellationToken::new();
        shutdown.cancel();
        let mut events = bulk_destroy_cancellable(
            client,
            fresh_cache(Vec::new()),
            Box::pin(stream::iter([ObjectId("m1".to_string())])),
            test_key(99),
            shutdown,
        );
        assert!(events.next().await.is_none());
        assert!(script.requests().is_empty());
    }

    fn scripted_client(steps: Vec<Canned>) -> (Arc<GmailClient>, Arc<ScriptedDispatch>) {
        let script = ScriptedDispatch::new(steps);
        let net = test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            RetryPolicy::disabled(),
        );
        (
            Arc::new(GmailClient::with_account_net("https://gmail.test", net)),
            script,
        )
    }

    fn fresh_cache(labels: Vec<crate::types::GmailLabel>) -> ScopeCache {
        Arc::new(super::super::scopes::ScopeCacheState::new(
            super::super::scopes::ScopeSnapshot {
                labels,
                fetched_at: Some(Instant::now()),
            },
        ))
    }

    fn test_key(sequence: u64) -> IdempotencyKey {
        IdempotencyKey {
            run_id: RunId("run".to_string()),
            sequence,
            protocol_salt: ProtocolSalt::Gmail("test".to_string()),
        }
    }

    fn set(items: &[&str]) -> HashSet<String> {
        items.iter().map(|item| (*item).to_string()).collect()
    }

    async fn collect_events(
        mut stream: AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>>,
    ) -> Vec<SyncEvent<ItemOutcome<MutationSuccess>>> {
        let mut events = Vec::new();
        while let Some(event) = stream.next().await {
            events.push(event);
        }
        events
    }

    fn label(id: &str) -> MembershipScope {
        MembershipScope::Label(LabelId(id.to_string()))
    }

    #[tokio::test]
    async fn batch_modify_not_found_bisects_and_only_fails_the_absent_id() {
        let (client, script) = scripted_client(vec![
            canned(StatusCode::NOT_FOUND),
            canned(StatusCode::NO_CONTENT),
            canned(StatusCode::NOT_FOUND),
            canned(StatusCode::NOT_FOUND),
            canned(StatusCode::NO_CONTENT),
        ]);
        let targets = Box::pin(stream::iter(
            ["live-1", "absent", "live-2"].map(|id| ObjectId(id.to_string())),
        ));
        let events = collect_events(bulk_move(
            client,
            fresh_cache(Vec::new()),
            targets,
            label("INBOX"),
            None,
            test_key(10),
        ))
        .await;

        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("mutation must produce an accounted batch");
        };
        assert_eq!(batch.items.len(), 3);
        assert!(
            matches!(&batch.items[0], ItemOutcome::Succeeded(success) if success.item.0 == "live-1")
        );
        assert!(
            matches!(&batch.items[1], ItemOutcome::Failed(failure) if failure.item.0 == "absent" && matches!(failure.error.kind(), bifrost_types::AccountErrorKind::NotFound(_)))
        );
        assert!(
            matches!(&batch.items[2], ItemOutcome::Succeeded(success) if success.item.0 == "live-2")
        );
        let requests = script.requests();
        assert_eq!(requests.len(), 5);
        assert_eq!(
            requests[1].body.as_deref(),
            Some(
                br#"{"ids":["live-1"],"addLabelIds":["INBOX"],"removeLabelIds":["SPAM","TRASH"]}"#
                    .as_slice()
            )
        );
    }

    #[tokio::test]
    async fn generic_batch_invalid_argument_is_not_bisected() {
        let bad_request = Canned::Response {
            status: StatusCode::BAD_REQUEST,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from_static(
                br#"{"error":{"code":400,"errors":[{"reason":"invalidArgument","message":"invalid request"}]}}"#,
            ),
        };
        let (client, script) = scripted_client(vec![bad_request]);
        let events = collect_events(bulk_move(
            client,
            fresh_cache(Vec::new()),
            Box::pin(stream::iter(
                ["m1", "m2"].map(|id| ObjectId(id.to_string())),
            )),
            label("INBOX"),
            None,
            test_key(16),
        ))
        .await;

        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("a complete bad-request response is classified per item");
        };
        assert_eq!(batch.items.len(), 2);
        assert!(batch.items.iter().all(|item| matches!(
            item,
            ItemOutcome::Failed(failure)
                if matches!(failure.error.kind(), bifrost_types::AccountErrorKind::Request(
                    bifrost_types::RequestErrorKind::Malformed
                ))
        )));
        assert_eq!(
            script.requests().len(),
            1,
            "a generic 400 can describe the shared patch, so it is not an id-level bisector signal"
        );
    }

    /// `GMAIL_BATCH_MODIFY_LIMIT` bounds the ids; nothing on the way in
    /// bounds the label lists, and `patch_for_set` names every user label
    /// in the account. An account past Gmail's documented per-update label
    /// cap must therefore be split into several bodies here - the 404
    /// bisector cannot help, since halving the id list leaves the label
    /// list exactly as long.
    #[test]
    fn a_label_list_over_the_cap_is_split_across_request_bodies() {
        let patch = LabelPatch {
            add_label_ids: (0..GMAIL_MODIFY_LABEL_LIMIT + 5)
                .map(|n| format!("Add_{n}"))
                .collect(),
            remove_label_ids: (0..GMAIL_MODIFY_LABEL_LIMIT * 2 + 1)
                .map(|n| format!("Rem_{n}"))
                .collect(),
            unsupported_flags: Vec::new(),
        };
        let ids = [ObjectId("m1".to_string()), ObjectId("m2".to_string())];
        let bodies = batch_modify_bodies(&ids, &patch);

        assert_eq!(bodies.len(), 3, "the longer list decides the body count");
        for body in &bodies {
            assert_eq!(body.ids, vec!["m1".to_string(), "m2".to_string()]);
            assert!(body.add_label_ids.len() <= GMAIL_MODIFY_LABEL_LIMIT);
            assert!(body.remove_label_ids.len() <= GMAIL_MODIFY_LABEL_LIMIT);
        }
        let added: Vec<String> = bodies
            .iter()
            .flat_map(|body| body.add_label_ids.clone())
            .collect();
        let removed: Vec<String> = bodies
            .iter()
            .flat_map(|body| body.remove_label_ids.clone())
            .collect();
        assert_eq!(
            added, patch.add_label_ids,
            "splitting must not drop or reorder a label"
        );
        assert_eq!(removed, patch.remove_label_ids);
    }

    /// A patch that fits stays exactly one request: chunking must not cost
    /// the common account an extra 50-unit call.
    #[test]
    fn a_label_list_under_the_cap_stays_one_request_body() {
        let patch = LabelPatch {
            add_label_ids: vec!["INBOX".to_string()],
            remove_label_ids: vec!["SPAM".to_string(), "TRASH".to_string()],
            unsupported_flags: Vec::new(),
        };
        let bodies = batch_modify_bodies(&[ObjectId("m1".to_string())], &patch);
        assert_eq!(bodies.len(), 1);
        assert_eq!(bodies[0].add_label_ids, patch.add_label_ids);
        assert_eq!(bodies[0].remove_label_ids, patch.remove_label_ids);
    }

    /// The same bound, reached through the real driver: a label-heavy
    /// account's exact-set reaches the wire as several capped requests
    /// and still reports one success per id.
    #[tokio::test]
    async fn exact_set_on_a_label_heavy_account_sends_capped_requests() {
        let (client, script) = scripted_client(vec![
            canned(StatusCode::NO_CONTENT),
            canned(StatusCode::NO_CONTENT),
            canned(StatusCode::NO_CONTENT),
        ]);
        let labels = (0..250)
            .map(|n| crate::types::GmailLabel {
                id: format!("Label_{n}"),
                name: format!("Name {n}"),
                label_type: Some("user".to_string()),
                color: None,
            })
            .collect::<Vec<_>>();
        let events = collect_events(bulk_set_flags(
            client,
            fresh_cache(labels),
            Box::pin(stream::iter([ObjectId("m1".to_string())])),
            FlagOp::Set(set(&["\\Seen"])),
            test_key(31),
        ))
        .await;

        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("a label-heavy exact set must still produce one accounted batch");
        };
        assert_eq!(batch.items.len(), 1);
        assert!(matches!(&batch.items[0], ItemOutcome::Succeeded(_)));

        let requests = script.requests();
        assert_eq!(
            requests.len(),
            3,
            "253 removals must not go out as one oversized body"
        );
        let mut removed = Vec::new();
        for request in &requests {
            let body: serde_json::Value =
                serde_json::from_slice(request.body.as_deref().expect("a body")).expect("json");
            assert_eq!(body["ids"], serde_json::json!(["m1"]));
            let chunk = body["removeLabelIds"]
                .as_array()
                .expect("removeLabelIds")
                .clone();
            assert!(
                chunk.len() <= GMAIL_MODIFY_LABEL_LIMIT,
                "every emitted body must stay under Gmail's per-update label cap"
            );
            removed.extend(
                chunk
                    .into_iter()
                    .map(|id| id.as_str().expect("id").to_string()),
            );
        }
        assert_eq!(
            removed.len(),
            253,
            "every user label plus STARRED / IMPORTANT / UNREAD still has to be removed"
        );
    }

    #[tokio::test]
    async fn a_later_chunk_404_bisects_ids_then_sends_remaining_chunks_to_live_ids() {
        let (client, script) = scripted_client(vec![
            canned(StatusCode::NO_CONTENT),
            canned(StatusCode::NOT_FOUND),
            canned(StatusCode::NO_CONTENT),
            canned(StatusCode::NOT_FOUND),
            canned(StatusCode::NOT_FOUND),
            canned(StatusCode::NO_CONTENT),
            canned(StatusCode::NO_CONTENT),
        ]);
        let labels = (0..250)
            .map(|n| crate::types::GmailLabel {
                id: format!("Label_{n}"),
                name: format!("Name {n}"),
                label_type: Some("user".to_string()),
                color: None,
            })
            .collect::<Vec<_>>();
        let events = collect_events(bulk_set_flags(
            client,
            fresh_cache(labels),
            Box::pin(stream::iter(
                ["live-1", "absent", "live-2"].map(|id| ObjectId(id.to_string())),
            )),
            FlagOp::Set(set(&["\\Seen"])),
            test_key(32),
        ))
        .await;

        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("chunked exact-set must produce an accounted batch");
        };
        assert!(matches!(
            batch.items.as_slice(),
            [
                ItemOutcome::Succeeded(first),
                ItemOutcome::Failed(missing),
                ItemOutcome::Succeeded(last),
            ] if first.item.0 == "live-1"
                && missing.item.0 == "absent"
                && matches!(missing.error.kind(), bifrost_types::AccountErrorKind::NotFound(_))
                && last.item.0 == "live-2"
        ));

        let requests = script.requests();
        assert_eq!(requests.len(), 7);
        let final_body: serde_json::Value = serde_json::from_slice(
            requests[6]
                .body
                .as_deref()
                .expect("last chunk request body"),
        )
        .expect("last chunk JSON");
        assert_eq!(
            final_body["ids"],
            serde_json::json!(["live-1", "live-2"]),
            "a 404 in a later label chunk must not drop the remaining chunk"
        );
    }

    #[tokio::test]
    async fn a_later_permanent_chunk_failure_keeps_partial_writes_uncertain() {
        let (client, script) = scripted_client(vec![
            canned(StatusCode::NO_CONTENT),
            canned(StatusCode::from_u16(418).expect("valid test status")),
        ]);
        let labels = (0..250)
            .map(|n| crate::types::GmailLabel {
                id: format!("Label_{n}"),
                name: format!("Name {n}"),
                label_type: Some("user".to_string()),
                color: None,
            })
            .collect::<Vec<_>>();
        let events = collect_events(bulk_set_flags(
            client,
            fresh_cache(labels),
            Box::pin(stream::iter([ObjectId("m1".to_string())])),
            FlagOp::Set(set(&["\\Seen"])),
            test_key(33),
        ))
        .await;

        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("a later chunk refusal must still produce an item lane");
        };
        assert!(matches!(
            batch.items.as_slice(),
            [ItemOutcome::Uncertain(item)] if item.item.0 == "m1"
        ));
        assert_eq!(
            script.requests().len(),
            2,
            "later chunks stop after refusal"
        );
    }

    #[tokio::test]
    async fn exact_set_never_sends_draft_or_sent_labels() {
        let (client, script) = scripted_client(vec![canned(StatusCode::NO_CONTENT)]);
        let labels = vec![crate::types::GmailLabel {
            id: "SENT".to_string(),
            name: "SENT".to_string(),
            label_type: Some("system".to_string()),
            color: None,
        }];
        let flags = set(&["\\Seen", "\\Draft", "$gmail-label:SENT:SENT"]);
        let events = collect_events(bulk_set_flags(
            client,
            fresh_cache(labels),
            Box::pin(stream::iter([ObjectId("m1".to_string())])),
            FlagOp::Set(flags),
            test_key(11),
        ))
        .await;

        // Gmail answers 400 for DRAFT or SENT in either label list, so
        // neither may reach the wire - but excluded-from-the-wire is not
        // absent-from-the-report. The requested draft/sent state was not
        // achieved, so both flags surface as unsupported and the id gets
        // the partial-application lane rather than a plain success.
        assert!(
            matches!(&events[0], SyncEvent::Warning(warning) if warning.kind == WarningKind::StrategyDowngraded)
        );
        let SyncEvent::Batch(batch) = &events[1] else {
            panic!("warning must be followed by the accounted mutation batch");
        };
        let [ItemOutcome::Succeeded(success)] = batch.items.as_slice() else {
            panic!("the representable half of the set was applied");
        };
        let MutationSuccess::Downgraded {
            actual: MutationEffect::FlagsPartiallyApplied { unsupported },
        } = &success.output
        else {
            panic!("a set Gmail could only half-apply must not report Applied");
        };
        assert_eq!(
            unsupported,
            &vec!["$gmail-label:SENT:SENT".to_string(), "\\Draft".to_string()]
        );
        let body: serde_json::Value =
            serde_json::from_slice(script.requests()[0].body.as_ref().expect("request body"))
                .expect("JSON request");
        assert_eq!(body["addLabelIds"], serde_json::json!([]));
        assert_eq!(
            body["removeLabelIds"],
            serde_json::json!(["IMPORTANT", "STARRED", "UNREAD"])
        );
    }

    /// `\Draft` alone leaves nothing Gmail can express, so no request is
    /// made - and an operation that made no request must not report any
    /// flavour of success. `Skipped` would claim the message was already
    /// as asked and `Downgraded` would claim it changed; both are false,
    /// and both are read by `bifrost-sync` and by five other protocol
    /// crates. The honest lane is a per-id `Unsupported` failure.
    #[tokio::test]
    async fn a_draft_only_flag_op_makes_no_request_and_reports_no_success() {
        let (client, script) = scripted_client(Vec::new());
        let events = collect_events(bulk_set_flags(
            client,
            fresh_cache(Vec::new()),
            Box::pin(stream::iter([ObjectId("m1".to_string())])),
            FlagOp::Add(set(&["\\Draft"])),
            test_key(14),
        ))
        .await;

        assert!(
            script.requests().is_empty(),
            "no representable label means nothing to send"
        );
        assert!(
            matches!(&events[0], SyncEvent::Warning(warning) if warning.kind == WarningKind::StrategyDowngraded)
        );
        let SyncEvent::Batch(batch) = &events[1] else {
            panic!("every id must still land in a lane");
        };
        let [ItemOutcome::Failed(failure)] = batch.items.as_slice() else {
            panic!("a no-op must not be reported as a success");
        };
        assert_eq!(failure.item.0, "m1");
        assert!(matches!(
            failure.error.kind(),
            bifrost_types::AccountErrorKind::Unsupported(AccountOperation::UpdateFlags)
        ));
        assert!(matches!(
            failure.error.recovery(),
            bifrost_types::RecoveryClass::Unsupported(_)
        ));
    }

    /// A terminal failure arriving mid-bisection must terminate the
    /// stream, not be laundered into per-id failures.
    ///
    /// The bisection path was a new error path, and the first cut of it
    /// converted every non-404 sub-batch error straight into
    /// `ItemOutcome::Failed`. A 429 read to the engine as "these items
    /// failed permanently" instead of "back off and retry" - a worse lie
    /// than the whole-batch `NotFound` fanout the bisection exists to
    /// fix. Sub-batches already resolved keep their lane in one last
    /// non-final page ahead of the terminator.
    #[tokio::test]
    async fn a_rate_limit_during_bisection_terminates_and_keeps_resolved_lanes() {
        let (client, script) = scripted_client(vec![
            canned(StatusCode::NOT_FOUND),
            canned(StatusCode::NO_CONTENT),
            canned(StatusCode::TOO_MANY_REQUESTS),
        ]);
        let targets = Box::pin(stream::iter(
            ["a", "b", "c", "d"].map(|id| ObjectId(id.to_string())),
        ));
        let events = collect_events(bulk_move(
            client,
            fresh_cache(Vec::new()),
            targets,
            label("INBOX"),
            None,
            test_key(15),
        ))
        .await;

        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("the sub-batch that succeeded must keep its lane");
        };
        assert_eq!(batch.items.len(), 2);
        assert!(batch.items.iter().all(|item| matches!(
            item,
            ItemOutcome::Succeeded(success) if success.output == MutationSuccess::Applied
        )));
        assert!(
            matches!(batch.page_boundary, PageBoundary::Page),
            "the operation did not complete, so no page is final"
        );
        let SyncEvent::Terminated(error) = &events[1] else {
            panic!("a rate limit must terminate the stream, not fail the ids");
        };
        assert!(matches!(
            error.recovery(),
            bifrost_types::RecoveryClass::Retry(_)
        ));
        assert_eq!(events.len(), 2, "a terminated stream emits no Done");
        assert_eq!(script.requests().len(), 3);
    }

    #[tokio::test]
    async fn unsupported_flag_downgrades_but_sends_representable_labels() {
        let (client, script) = scripted_client(vec![canned(StatusCode::NO_CONTENT)]);
        let flags = set(&["\\Seen", "\\Flagged", "$Junk"]);
        let events = collect_events(bulk_set_flags(
            client,
            fresh_cache(Vec::new()),
            Box::pin(stream::iter([ObjectId("m1".to_string())])),
            FlagOp::Add(flags),
            test_key(12),
        ))
        .await;

        assert!(
            matches!(&events[0], SyncEvent::Warning(warning) if warning.kind == WarningKind::StrategyDowngraded)
        );
        let SyncEvent::Batch(batch) = &events[1] else {
            panic!("warning must be followed by the accounted mutation batch");
        };
        assert!(
            matches!(batch.items.as_slice(), [ItemOutcome::Succeeded(success)] if matches!(&success.output, MutationSuccess::Downgraded { actual: MutationEffect::FlagsPartiallyApplied { unsupported } } if unsupported == &["$Junk".to_string()]))
        );
        let body: serde_json::Value =
            serde_json::from_slice(script.requests()[0].body.as_ref().expect("request body"))
                .expect("JSON request");
        assert_eq!(body["addLabelIds"], serde_json::json!(["STARRED"]));
        assert_eq!(body["removeLabelIds"], serde_json::json!(["UNREAD"]));
    }

    #[tokio::test]
    async fn destroy_scope_failure_sends_trash_fallback_and_reports_downgraded() {
        let forbidden = Canned::Response {
            status: StatusCode::FORBIDDEN,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from_static(
                br#"{"error":{"code":403,"errors":[{"reason":"forbidden"}]}}"#,
            ),
        };
        let (client, script) = scripted_client(vec![forbidden, canned(StatusCode::NO_CONTENT)]);
        let events = collect_events(bulk_destroy(
            client,
            fresh_cache(Vec::new()),
            Box::pin(stream::iter([ObjectId("m1".to_string())])),
            test_key(13),
        ))
        .await;

        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("destroy fallback must produce an accounted batch");
        };
        assert!(
            matches!(batch.items.as_slice(), [ItemOutcome::Succeeded(success)] if success.output == MutationSuccess::Downgraded {
                actual: MutationEffect::MovedToContainer(ContainerId("TRASH".to_string())),
            })
        );
        let requests = script.requests();
        assert_eq!(requests.len(), 2);
        assert_eq!(requests[0].url.path(), "/messages/batchDelete");
        assert_eq!(requests[1].url.path(), "/messages/batchModify");
        assert_eq!(
            requests[1].body.as_deref(),
            Some(
                br#"{"ids":["m1"],"addLabelIds":["TRASH"],"removeLabelIds":["INBOX","SPAM"]}"#
                    .as_slice()
            )
        );
    }

    /// The trash fallback reports `Downgraded`, and only for ids it actually
    /// trashed.
    ///
    /// `bulk_destroy` falls back to a TRASH patch when `batchDelete` is refused
    /// for scope reasons - the ordinary case under the `gmail.modify` OAuth
    /// scope, which does not permit permanent delete. Those messages moved but
    /// still exist, so taking the patch's own `Applied` outcomes told the engine
    /// they were destroyed and earned a permanent destroy/reappear reconcile
    /// loop.
    ///
    /// The second assertion is the one that is easy to get wrong: an id whose
    /// trash patch FAILED was not downgraded, it was not mutated at all, and
    /// its classified failure is the honest answer. Re-labelling every id from
    /// the input list would bury those failures as successes-of-a-weaker-kind.
    #[test]
    fn the_destroy_trash_fallback_downgrades_only_what_it_trashed() {
        use bifrost_types::BatchFailure;

        let failure = crate::account::error::into_account_error(
            crate::Error::invalid_request(AccountOperation::BulkDestroy, "trash patch refused"),
            GmailErrorContext::mutation(AccountOperation::BulkDestroy),
        );
        let ids = [ObjectId("trashed".into()), ObjectId("refused".into())];
        let mut progress = MutationProgress::new(&ids, 1);
        progress.completed_effect = Some(MutationEffect::MovedToContainer(ContainerId(
            LABEL_TRASH.to_string(),
        )));
        progress.acknowledge(&ids[..1], 0, &[]);
        progress.finish_failures(
            vec![ItemOutcome::Failed(BatchFailure::new(
                BatchItemId("refused".into()),
                failure,
            ))],
            false,
        );
        let downgraded = progress.completed_outcomes();

        match &downgraded[0] {
            ItemOutcome::Succeeded(success) => assert_eq!(
                success.output,
                MutationSuccess::Downgraded {
                    actual: MutationEffect::MovedToContainer(ContainerId("TRASH".to_string())),
                },
                "a trashed message was not destroyed and must report the state it applied"
            ),
            other => panic!("expected a succeeded outcome, got {other:?}"),
        }
        assert!(
            matches!(&downgraded[1], ItemOutcome::Failed(_)),
            "an id whose trash patch failed was not downgraded, it was not mutated"
        );
    }

    #[test]
    fn move_to_inbox_clears_spam_and_trash() {
        let patch = move_patch(&label("INBOX"), None);
        assert_eq!(patch.add_label_ids, vec!["INBOX".to_string()]);
        assert_eq!(
            patch.remove_label_ids,
            vec!["SPAM".to_string(), "TRASH".to_string()],
            "bulk un-spam must strip SPAM, not merely add INBOX"
        );
        assert!(patch.unsupported_flags.is_empty());
    }

    #[test]
    fn move_to_inbox_is_case_insensitive() {
        let patch = move_patch(&label("inbox"), None);
        assert_eq!(
            patch.remove_label_ids,
            vec!["SPAM".to_string(), "TRASH".to_string()]
        );
    }

    #[test]
    fn move_to_user_label_clears_every_exclusive_container() {
        let patch = move_patch(&label("Label_42"), None);
        assert_eq!(patch.add_label_ids, vec!["Label_42".to_string()]);
        assert_eq!(
            patch.remove_label_ids,
            vec!["INBOX".to_string(), "SPAM".to_string(), "TRASH".to_string()],
            "filing a spammed message into a label must take it out of Spam"
        );
    }

    #[test]
    fn bulk_move_to_archive_adds_no_label() {
        let patch = move_patch(&label("archive"), None);
        assert!(
            patch.add_label_ids.is_empty(),
            "`archive` is synthetic; asking Gmail to apply it is a 400"
        );
        assert_eq!(
            patch.remove_label_ids,
            vec!["INBOX".to_string(), "SPAM".to_string(), "TRASH".to_string()]
        );
        assert!(patch.unsupported_flags.is_empty());
    }

    #[test]
    fn source_label_rides_the_same_batch_modify() {
        let patch = move_patch(&label("Label_42"), Some(&label("Label_7")));
        assert_eq!(patch.add_label_ids, vec!["Label_42".to_string()]);
        assert_eq!(
            patch.remove_label_ids,
            vec![
                "INBOX".to_string(),
                "SPAM".to_string(),
                "TRASH".to_string(),
                "Label_7".to_string()
            ],
            "the source detach must not cost a second request"
        );
    }

    #[test]
    fn source_already_implied_by_the_destination_is_not_repeated() {
        let patch = move_patch(&label("Label_42"), Some(&label("inbox")));
        assert_eq!(
            patch.remove_label_ids,
            vec!["INBOX".to_string(), "SPAM".to_string(), "TRASH".to_string()]
        );
    }

    #[test]
    fn synthetic_archive_source_is_dropped() {
        let patch = move_patch(&label("Label_42"), Some(&label("archive")));
        assert_eq!(
            patch.remove_label_ids,
            vec!["INBOX".to_string(), "SPAM".to_string(), "TRASH".to_string()],
            "`archive` is not a removable Gmail label"
        );
    }

    #[test]
    fn source_equal_to_destination_is_dropped() {
        let patch = move_patch(&label("Label_42"), Some(&label("Label_42")));
        assert_eq!(patch.add_label_ids, vec!["Label_42".to_string()]);
        assert!(
            !patch.remove_label_ids.contains(&"Label_42".to_string()),
            "one request must not both add and remove the same label"
        );
    }

    #[test]
    fn non_label_destination_is_unsupported() {
        let patch = move_patch(&MembershipScope::Folder(FolderId("INBOX".into())), None);
        assert!(patch.add_label_ids.is_empty());
        assert!(patch.remove_label_ids.is_empty());
        assert_eq!(patch.unsupported_flags.len(), 1);
    }

    #[test]
    fn non_label_source_is_unsupported() {
        let patch = move_patch(
            &label("Label_42"),
            Some(&MembershipScope::Folder(FolderId("INBOX".into()))),
        );
        assert!(patch.add_label_ids.is_empty());
        assert!(patch.remove_label_ids.is_empty());
        assert_eq!(patch.unsupported_flags.len(), 1);
    }

    #[tokio::test]
    async fn non_label_move_fails_each_id_and_marks_the_last_batch_final() {
        let client = Arc::new(GmailClient::new("token"));
        let targets: AccountStream<ObjectId> = Box::pin(stream::iter([ObjectId("m1".to_string())]));
        let mut events = bulk_move(
            client,
            Arc::new(super::super::scopes::ScopeCacheState::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            targets,
            MembershipScope::Folder(FolderId("INBOX".to_string())),
            None,
            IdempotencyKey {
                run_id: RunId("run".to_string()),
                sequence: 1,
                protocol_salt: ProtocolSalt::Gmail("test".to_string()),
            },
        );

        let first = events.next().await.expect("mutation batch");
        let SyncEvent::Batch(batch) = first else {
            panic!("invalid destination should produce per-id outcomes");
        };
        assert!(matches!(batch.page_boundary, PageBoundary::Final));
        assert!(matches!(batch.items.as_slice(), [ItemOutcome::Failed(_)]));
        assert!(matches!(events.next().await, Some(SyncEvent::Done(None))));
        assert!(events.next().await.is_none());
    }

    #[tokio::test]
    async fn mutation_lookahead_marks_only_the_last_batch_final() {
        let client = Arc::new(GmailClient::new("token"));
        let targets: AccountStream<ObjectId> = Box::pin(stream::iter(
            (0..=GMAIL_BATCH_MODIFY_LIMIT).map(|index| ObjectId(format!("m{index}"))),
        ));
        let mut events = bulk_move(
            client,
            Arc::new(super::super::scopes::ScopeCacheState::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            targets,
            MembershipScope::Folder(FolderId("INBOX".to_string())),
            None,
            IdempotencyKey {
                run_id: RunId("run".to_string()),
                sequence: 2,
                protocol_salt: ProtocolSalt::Gmail("test".to_string()),
            },
        );

        let Some(SyncEvent::Batch(first)) = events.next().await else {
            panic!("first mutation page");
        };
        assert_eq!(first.items.len(), GMAIL_BATCH_MODIFY_LIMIT);
        assert!(matches!(first.page_boundary, PageBoundary::Page));

        let Some(SyncEvent::Batch(last)) = events.next().await else {
            panic!("last mutation page");
        };
        assert_eq!(last.items.len(), 1);
        assert!(matches!(last.page_boundary, PageBoundary::Final));
        assert!(matches!(events.next().await, Some(SyncEvent::Done(None))));
    }
}
