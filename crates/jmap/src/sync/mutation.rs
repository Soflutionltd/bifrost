use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountOperation, AccountStream, Batch, BatchFailure, BatchItemId, ErrorScope, FlagOp,
    ItemOutcome, MembershipScope, MutationSuccess, ObjectId, PageBoundary, SyncEvent,
};
use futures::StreamExt;

use crate::core::transport::HttpTransport;
use crate::email::{EmailId, EmailPatch, EmailSet};
use crate::mailbox::MailboxId;

use super::mail_set::classify_set_response;

type MailAccount<T> = super::registry::Slot<T>;

pub(crate) fn set_flags<T: HttpTransport>(
    foreign_mail: Arc<crate::sync::registry::Registry<T>>,
    targets: AccountStream<ObjectId>,
    op: FlagOp,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    if let Err(error) = op.validate_for_account(bifrost_types::Protocol::Jmap) {
        return Box::pin(futures::stream::once(async move {
            SyncEvent::Terminated(error)
        }));
    }
    // `validate_for_account` has already rejected every empty additive,
    // subtractive, and wholly empty patch operation, so no no-op flag
    // mutation reaches the routing match below.
    match &op {
        FlagOp::Add(_) | FlagOp::Remove(_) | FlagOp::Set(_) | FlagOp::Patch { .. } => {
            mutation_stream(foreign_mail, targets, MutationKind::Flags(op))
        }
        _ => Box::pin(async_stream::stream! {
            yield super::error::terminated_unsupported(
                AccountOperation::UpdateFlags,
                None,
                bifrost_types::UnsupportedReason::FlagOperation,
                "JMAP does not support this FlagOp variant",
            );
        }),
    }
}

pub(crate) fn move_to<T: HttpTransport>(
    foreign_mail: Arc<crate::sync::registry::Registry<T>>,
    targets: AccountStream<ObjectId>,
    destination: MembershipScope,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    let kind = match destination {
        MembershipScope::Mailbox(mailbox) => MutationKind::Move(MailboxId::new(mailbox.0)),
        _ => {
            return Box::pin(async_stream::stream! {
                yield super::error::terminated(super::error::unsupported_error(
                    bifrost_types::AccountOperation::BulkMove,
                    None,
                    bifrost_types::UnsupportedReason::RequestShape,
                    "JMAP bulk_move only supports MembershipScope::Mailbox",
                ));
            });
        }
    };
    mutation_stream(foreign_mail, targets, kind)
}

pub(crate) fn destroy<T: HttpTransport>(
    foreign_mail: Arc<crate::sync::registry::Registry<T>>,
    targets: AccountStream<ObjectId>,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    mutation_stream(foreign_mail, targets, MutationKind::Destroy)
}

// RFC 8620 Section 5.3 requires a mismatching ifInState to reject the
// entire /set without changes. The token is for (accountId, object type),
// not an object's ETag or a queryState. Never refresh a caller's token.
// https://www.rfc-editor.org/rfc/rfc8620.html#section-5.3
// RFC 9610 Section 3.5 (ContactCard/set) and calendars draft-26 Section
// 5.9 (CalendarEvent/set) explicitly adopt this standard /set contract:
// https://www.rfc-editor.org/rfc/rfc9610.html#section-3.5
// https://datatracker.ietf.org/doc/html/draft-ietf-jmap-calendars-26#section-5.9
pub(super) fn caller_state<'a>(
    version: Option<&'a bifrost_types::ServerVersion>,
    operation: AccountOperation,
    account_id: &str,
) -> Result<Option<&'a str>, bifrost_types::AccountError> {
    version
        .map(|version| version.expected_jmap_state(account_id, operation))
        .transpose()
}

pub(super) fn versioned_stream<T: HttpTransport>(
    foreign_mail: Arc<crate::sync::registry::Registry<T>>,
    targets: AccountStream<bifrost_types::MailMutationTarget>,
    kind: MutationKind,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    let mail = foreign_mail.primary.clone();
    let limits = mail.limits;

    Box::pin(async_stream::stream! {
        // RFC 8620 Section 5.3 guards the entire Email/set. Submitting each
        // target separately would let the first success invalidate siblings
        // carrying the same caller state. Group by owner AND caller state,
        // within the advertised object limit, without advancing caller tokens.
        // ready_chunks flushes when input is pending, so a live producer does
        // not have to fill the limit before seeing a result.
        let mut chunks = targets.ready_chunks(super::capabilities::set_batch_size(limits.max_objects_in_set.get()));
        while let Some(chunk) = chunks.next().await {
            let mut groups: Vec<(String, Option<String>, Vec<ObjectId>)> = Vec::new();
            let mut rejected = Vec::new();
            for target in chunk {
                let owner = foreign_mail.object(&target.id.0).id_str().to_owned();
                let validation = if let MutationKind::Flags(op) = &kind {
                    op.validate_for_account(bifrost_types::Protocol::Jmap)
                } else {
                    Ok(())
                };
                let expected = validation.and_then(|()| {
                    caller_state(target.expected_version.as_ref(), operation_for_kind(&kind), &owner)
                        .map(|state| state.map(str::to_owned))
                });
                let expected = match expected {
                    Ok(expected) => expected,
                    Err(error) => {
                        rejected.push(ItemOutcome::Failed(BatchFailure::new(
                            BatchItemId(target.id.0), error,
                        )));
                        continue;
                    }
                };
                if let Some(outcome) = destination_rejection(&kind, &target.id) {
                    rejected.push(outcome);
                    continue;
                }
                if let Some((_, _, ids)) = groups.iter_mut().find(|(account, state, _)| {
                    account == &owner && state == &expected
                }) {
                    ids.push(target.id);
                } else {
                    groups.push((owner, expected, vec![target.id]));
                }
            }
            if !rejected.is_empty() {
                yield SyncEvent::Batch(Batch {
                    items: rejected,
                    page_boundary: PageBoundary::Page,
                    server_latency: std::time::Duration::ZERO,
                    bytes_in: 0,
                    checkpoint: None,
                });
            }
            for (account_id, expected, mut ids) in groups {
                let owner = foreign_mail.slot_by_id(&crate::core::id::AccountId::new(&account_id)).expect("buffered account is registered");
                match apply_batch_with_version(owner, &kind, &mut ids, expected.as_deref()).await {
                    Ok(Some(batch)) => yield SyncEvent::Batch(batch),
                    Ok(None) => {},
                    Err(error) => {
                        yield super::error::terminated(super::error::into_account_error(
                            error, super::error::JmapErrorContext::new(operation_for_kind(&kind)),
                        ));
                        return;
                    }
                }
            }
        }
        yield SyncEvent::Done(None);
    })
}

#[derive(Clone)]
pub(super) enum MutationKind {
    Flags(FlagOp),
    Move(MailboxId),
    Destroy,
}

fn operation_for_kind(kind: &MutationKind) -> AccountOperation {
    match kind {
        MutationKind::Flags(_) => AccountOperation::UpdateFlags,
        MutationKind::Move(_) => AccountOperation::BulkMove,
        MutationKind::Destroy => AccountOperation::BulkDestroy,
    }
}

/// One owner account's pending work, held between flushes.
///
/// `last_touch` is the input position at which this route last received a
/// target. It is what bounds a partial batch's wait: without it a route that
/// fell one target short of `batch_size` would sit in the map until the whole
/// input stream ended, so an owner that trickles could be starved
/// indefinitely by an owner that streams.
#[derive(Default)]
struct PendingRoute {
    /// Targets bound for this account's `Email/set`, in arrival order.
    items: Vec<ObjectId>,
    /// Outcomes already decided locally (a destination this account cannot
    /// express). They ride out in the same batch as the wire results so the
    /// caller still sees exactly one outcome per input target.
    rejected: Vec<ItemOutcome<MutationSuccess>>,
    /// Input position of the most recent target routed here.
    last_touch: usize,
}

impl PendingRoute {
    fn pending(&self) -> usize {
        self.items.len() + self.rejected.len()
    }

    fn is_empty(&self) -> bool {
        self.pending() == 0
    }
}

/// Per-owner batching for one bulk mutation stream.
///
/// This stays mutation-specific. Its `last_touch` is an input-count fairness
/// bound, and each owner carries both wire targets and locally rejected
/// outcomes. Hydration has no rejection lane and must wake on elapsed
/// `BatchingPolicy.max_wait` while its input is pending; sharing would turn
/// this helper into a timer-driven generic queue and change mutation's
/// batching semantics. Keep the hydration timer local until the policy is
/// carried through both account paths.
///
/// Two properties the flat `HashMap` + `Vec<String>` it replaced did not
/// have, both of which only bite on multi-account (shared-mailbox) input:
///
/// * `order` holds each owner exactly ONCE. Draining empties a route in
///   place instead of removing it, so a re-fed owner does not append a
///   second entry. Removing the entry made `order` grow by one `String` per
///   input target whenever `batch_size` was 1, i.e. unbounded in the input.
/// * A partial batch is flushed once `batch_size` further targets have gone
///   to other owners. Waiting for end-of-input instead let one owner's
///   stream hold another owner's short batch for the entire run.
struct RouteBuffers {
    batch_size: usize,
    /// Count of targets accepted so far; the clock `last_touch` is read on.
    seen: usize,
    /// Distinct owners in first-arrival order.
    order: Vec<String>,
    pending: HashMap<String, PendingRoute>,
}

impl RouteBuffers {
    fn new(batch_size: usize) -> Self {
        Self {
            batch_size,
            seen: 0,
            order: Vec::new(),
            pending: HashMap::new(),
        }
    }

    /// Record one routed target and report the owners due for flush now:
    /// this owner if its batch just filled, plus any owner sitting on a
    /// partial batch that has gone `batch_size` inputs without being fed.
    fn push(
        &mut self,
        account_id: String,
        target: ObjectId,
        rejection: Option<ItemOutcome<MutationSuccess>>,
    ) -> Vec<String> {
        self.seen += 1;
        let seen = self.seen;
        let batch_size = self.batch_size;
        let order = &mut self.order;
        let route = self.pending.entry(account_id.clone()).or_insert_with(|| {
            order.push(account_id.clone());
            PendingRoute::default()
        });
        route.last_touch = seen;
        match rejection {
            Some(outcome) => route.rejected.push(outcome),
            None => route.items.push(target),
        }
        let full = route.pending() >= batch_size;

        let mut due: Vec<String> = self
            .order
            .iter()
            .filter(|owner| {
                self.pending.get(*owner).is_some_and(|route| {
                    !route.is_empty() && seen.saturating_sub(route.last_touch) >= batch_size
                })
            })
            .cloned()
            .collect();
        if full {
            due.push(account_id);
        }
        due
    }

    /// Every owner still holding work, in first-arrival order. Used for the
    /// end-of-input drain.
    fn remaining(&self) -> Vec<String> {
        self.order.clone()
    }

    /// Take one owner's pending work, leaving its route in place so the
    /// owner is never re-registered in `order`.
    fn drain(
        &mut self,
        account_id: &str,
    ) -> Option<(Vec<ObjectId>, Vec<ItemOutcome<MutationSuccess>>)> {
        let route = self.pending.get_mut(account_id)?;
        if route.is_empty() {
            return None;
        }
        Some((
            std::mem::take(&mut route.items),
            std::mem::take(&mut route.rejected),
        ))
    }

    /// Return every locally decided outcome still buffered, preserving the
    /// order in which its owner first appeared. A terminal wire failure must
    /// not erase decisions that needed no wire response.
    fn drain_rejections(&mut self) -> Vec<ItemOutcome<MutationSuccess>> {
        let mut rejected = Vec::new();
        for owner in &self.order {
            if let Some(route) = self.pending.get_mut(owner) {
                rejected.append(&mut route.rejected);
            }
        }
        rejected
    }
}

fn mutation_stream<T: HttpTransport>(
    foreign_mail: Arc<crate::sync::registry::Registry<T>>,
    mut targets: AccountStream<ObjectId>,
    kind: MutationKind,
) -> AccountStream<SyncEvent<ItemOutcome<MutationSuccess>>> {
    let mail = foreign_mail.primary.clone();
    let limits = mail.limits;

    Box::pin(async_stream::stream! {
        let batch_size = super::capabilities::set_batch_size(limits.max_objects_in_set.get());
        let mut buffers = RouteBuffers::new(batch_size);

        loop {
            let (due, at_end) = match targets.next().await {
                Some(target) => {
                    let account_id =
                        foreign_mail.object(&target.0).id_str().to_owned();
                    let rejection = destination_rejection(&kind, &target);
                    (buffers.push(account_id, target, rejection), false)
                }
                None => (buffers.remaining(), true),
            };

            for account_id in due {
                let Some((mut batch, rejected)) = buffers.drain(&account_id) else {
                    continue;
                };
                let routed_mail = foreign_mail.slot_by_id(&crate::core::id::AccountId::new(&account_id)).expect("buffered account is registered");
                let started = Instant::now();
                match apply_batch(routed_mail, &kind, &mut batch)
                .await
                {
                    Ok(Some(mut out)) => {
                        out.items.extend(rejected);
                        yield SyncEvent::Batch(out);
                    }
                    Ok(None) => {
                        if !rejected.is_empty() {
                            yield SyncEvent::Batch(Batch {
                                items: rejected,
                                page_boundary: PageBoundary::Page,
                                server_latency: started.elapsed(),
                                // Locally rejected before any wire call:
                                // `apply_batch` returned `None` precisely
                                // because it had nothing to send.
                                bytes_in: 0,
                                checkpoint: None,
                            });
                        }
                    }
                    Err(err) => {
                        let mut rejected = rejected;
                        rejected.extend(buffers.drain_rejections());
                        if !rejected.is_empty() {
                            yield SyncEvent::Batch(Batch {
                                items: rejected,
                                page_boundary: PageBoundary::Page,
                                server_latency: started.elapsed(),
                                bytes_in: 0,
                                checkpoint: None,
                            });
                        }
                        yield super::error::terminated_from_jmap(
                            err,
                            super::error::JmapErrorContext::new(operation_for_kind(&kind)),
                        );
                        return;
                    }
                }
            }

            if at_end {
                break;
            }
        }

        yield SyncEvent::Done(None);
    })
}

/// Reject a target whose owner disagrees with the move destination's owner
/// before it can reach the wire.
///
/// The destination is one `MembershipScope` for the whole call while targets
/// are routed per owner, so the two can disagree per target. `Email/set`
/// resolves both operands inside the routed `accountId`, which makes the
/// disagreement silent rather than loud: a bare primary mailbox id sent
/// against a foreign account resolves to whatever mailbox that account has
/// under that id. Fail the individual target, not the stream - the caller's
/// other targets may be perfectly expressible.
fn destination_rejection(
    kind: &MutationKind,
    target: &ObjectId,
) -> Option<ItemOutcome<MutationSuccess>> {
    let MutationKind::Move(destination) = kind else {
        return None;
    };
    let target_ref = super::foreign::JmapRef::parse(&target.0);
    let destination_ref = super::foreign::JmapRef::parse(destination.as_str());
    let target_owner = target_ref.owner.shared();
    let destination_owner = destination_ref.owner.shared();
    if target_ref.same_owner(destination_ref) {
        return None;
    }
    Some(ItemOutcome::Failed(BatchFailure::new(
        BatchItemId(target.0.clone()),
        super::error::cross_account_destination(
            AccountOperation::BulkMove,
            &target.0,
            target_owner,
            destination.as_str(),
            destination_owner,
        ),
    )))
}

async fn apply_batch<T: HttpTransport>(
    mail: &MailAccount<T>,
    kind: &MutationKind,
    batch: &mut Vec<ObjectId>,
) -> crate::Result<Option<Batch<ItemOutcome<MutationSuccess>>>> {
    apply_batch_with_version(mail, kind, batch, None).await
}

async fn apply_batch_with_version<T: HttpTransport>(
    mail: &MailAccount<T>,
    kind: &MutationKind,
    batch: &mut Vec<ObjectId>,
    expected: Option<&str>,
) -> crate::Result<Option<Batch<ItemOutcome<MutationSuccess>>>> {
    let started = Instant::now();
    let ids = std::mem::take(batch);
    if ids.is_empty() {
        // A route can hold only locally-rejected targets. Probing Email
        // state for it would spend a round trip to send nothing.
        return Ok(None);
    }
    // One emitted batch here can cost up to four `/jmap/api` POSTs: a
    // state probe, the `Email/set`, a re-probe after a `stateMismatch`,
    // and the retried set. The accumulator is what makes the conflict
    // path report its real cost instead of only the last call's.
    let (mail, tally) = mail.metered();
    let mail = &mail;
    let expected = expected.map(crate::core::id::ObjectState::<crate::email::Email>::new);
    let guard = match expected.as_ref() {
        Some(state) => super::mail_set::Guard::Exact(state),
        None if uses_if_in_state(kind) => super::mail_set::Guard::Replacement,
        None => super::mail_set::Guard::Unconditional,
    };
    let guarded = guard.is_guarded();
    let mut response = match super::mail_set::execute(mail, guard, || {
        build_email_set(None, &ids, kind, mail.owner())
    })
    .await
    {
        Ok(response) => response,
        Err(error) if guarded && super::error::is_state_mismatch(&error) => {
            return Ok(Some(state_mismatch_failed_batch(
                error,
                kind,
                ids,
                started,
                tally.take(),
            )));
        }
        Err(error) => return Err(error),
    };

    let operation = operation_for_kind(kind);
    let mut results = Vec::with_capacity(ids.len());
    for id in ids {
        let email_id = EmailId::new(mail.wire(&id.0));
        let raw = match kind {
            MutationKind::Destroy => response.destroyed(&email_id),
            MutationKind::Flags(_) | MutationKind::Move(_) => {
                response.updated(&email_id).map(|_| ())
            }
        };
        results.push(classify_set_response(raw, id, operation));
    }

    Ok(Some(Batch {
        items: results,
        page_boundary: PageBoundary::Page,
        server_latency: started.elapsed(),
        bytes_in: tally.take(),
        checkpoint: None,
    }))
}

/// Build a per-item `Failed(ConcurrencyConflict)` batch for every id in
/// `ids` from a surviving method-level `stateMismatch`. Each id gets its
/// own `AccountError` (scoped to that message) routed through
/// `into_account_error`, which classifies the `stateMismatch` as
/// `ConcurrencyConflict -> Retry::AfterStateRefresh`. This keeps the bulk
/// stream alive: a method-level conflict is per-item recoverable, not a
/// whole-stream terminator.
fn state_mismatch_failed_batch(
    err: crate::Error,
    kind: &MutationKind,
    ids: Vec<ObjectId>,
    started: Instant,
    bytes_in: u64,
) -> Batch<ItemOutcome<MutationSuccess>> {
    let operation = operation_for_kind(kind);
    let mut results = Vec::with_capacity(ids.len());
    let mut err = Some(err);
    for id in ids {
        // `into_account_error` consumes the error; clone the typed
        // method error per id so every lane entry carries a full,
        // correctly-scoped `ConcurrencyConflict` classification.
        let item_err = match err.take() {
            Some(e) => e,
            None => crate::Error::Method(state_mismatch_method_error()),
        };
        let account_error = super::error::into_account_error(
            item_err,
            super::error::JmapErrorContext::new(operation).with_scope(ErrorScope::Message {
                id: (id.0.clone()).into(),
            }),
        );
        results.push(ItemOutcome::Failed(BatchFailure::new(
            BatchItemId(id.0.clone()),
            account_error,
        )));
    }
    Batch {
        items: results,
        page_boundary: PageBoundary::Page,
        server_latency: started.elapsed(),
        bytes_in,
        checkpoint: None,
    }
}

/// A synthetic `stateMismatch` method error for the second-and-later ids
/// in a conflicted batch (the wire error is consumed by the first id).
fn state_mismatch_method_error() -> crate::core::error::MethodError {
    serde_json::from_str(r#"{"type":"stateMismatch"}"#)
        .expect("stateMismatch is a valid MethodError shape")
}

fn build_email_set(
    state: Option<&str>,
    ids: &[ObjectId],
    kind: &MutationKind,
    owner: crate::sync::foreign::Owner<'_>,
) -> crate::Result<EmailSet> {
    let mut set = EmailSet::new();
    if let Some(state) = state {
        set = set.if_in_state(state.to_string());
    }

    match kind {
        MutationKind::Destroy => {
            set = set.destroy(ids.iter().map(|id| wire_email_id(id, owner)));
        }
        MutationKind::Flags(op) => {
            for id in ids {
                apply_flags(set.update(wire_email_id(id, owner)), op)?;
            }
        }
        MutationKind::Move(mailbox_id) => {
            for id in ids {
                set.update(wire_email_id(id, owner))
                    .mailbox_ids([wire_mailbox_id(mailbox_id, owner)]);
            }
        }
    }

    Ok(set)
}

/// Account-wide Email state is useful for replacements, which could discard
/// concurrent edits to the whole property. Dotted keyword updates and
/// destroy do not need that guard: unrelated account changes cannot make
/// those requested operations stale. Bulk move replaces the whole
/// `mailboxIds` map, so it retains the state check too.
fn uses_if_in_state(kind: &MutationKind) -> bool {
    matches!(kind, MutationKind::Move(_)) || matches!(kind, MutationKind::Flags(FlagOp::Set(_)))
}

/// Convert a routed foreign object back to the native id JMAP accepts on the
/// selected account. Keep an id for an unregistered foreign account intact on
/// the primary route, preserving the real server-side not-found behavior.
fn wire_email_id(id: &ObjectId, owner: crate::sync::foreign::Owner<'_>) -> EmailId {
    EmailId::new(super::foreign::JmapRef::parse(&id.0).wire(owner))
}

/// The native form of the move destination for the routed account.
/// `destination_rejection` has already established that the destination and
/// every target in this batch name the same owner, so the only work left is
/// stripping the qualification the foreign account does not use.
fn wire_mailbox_id(id: &MailboxId, owner: crate::sync::foreign::Owner<'_>) -> MailboxId {
    MailboxId::new(super::foreign::JmapRef::parse(id.as_str()).wire(owner))
}

fn apply_flags(patch: &mut EmailPatch, op: &FlagOp) -> crate::Result<()> {
    match op {
        FlagOp::Add(flags) => {
            for flag in flags {
                patch.keyword(&crate::email::keyword::Keyword::parse(flag)?, true);
            }
        }
        FlagOp::Remove(flags) => {
            for flag in flags {
                patch.keyword(&crate::email::keyword::Keyword::parse(flag)?, false);
            }
        }
        FlagOp::Set(flags) => {
            patch.keywords(
                flags
                    .iter()
                    .map(crate::email::keyword::Keyword::parse)
                    .collect::<crate::Result<Vec<_>>>()?,
            );
        }
        FlagOp::Patch { add, remove } => {
            for flag in add {
                patch.keyword(&crate::email::keyword::Keyword::parse(flag)?, true);
            }
            for flag in remove {
                patch.keyword(&crate::email::keyword::Keyword::parse(flag)?, false);
            }
        }
        // `set_flags` refuses unknown variants before building the
        // mutation stream. Retain a second refusal here so a future internal
        // caller cannot turn an unhandled operation into a successful no-op.
        _ => {
            return Err(crate::Error::UnsupportedFlagOperation(
                "JMAP flag patch received an unsupported FlagOp variant",
            ));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sync::capabilities::ObjectLimits;
    use bifrost_types::{AccountErrorKind, RecoveryClass, RetryDisposition};
    use futures::StreamExt;
    use std::sync::Mutex as StdMutex;

    /// Serialize the exact builder used by the shared executor, so a request-shape
    /// regression cannot pass against a parallel test-only implementation.
    fn set_body(kind: &MutationKind, ids: &[&str], state: Option<&str>) -> serde_json::Value {
        let ids = ids
            .iter()
            .map(|id| ObjectId((*id).to_string()))
            .collect::<Vec<_>>();
        serde_json::to_value(
            build_email_set(state, &ids, kind, crate::sync::foreign::Owner::Primary)
                .expect("supported mutation builds an Email/set"),
        )
        .expect("Email/set serializes")
    }

    fn flags(values: &[&str]) -> bifrost_types::FlagSet {
        values.iter().map(|value| String::from(*value)).collect()
    }

    #[derive(Clone)]
    struct FailingSetTransport;

    impl HttpTransport for FailingSetTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
            _replay: bifrost_net::ReplayPolicy,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: serde_json::Value =
                serde_json::from_slice(&body).expect("request is JSON");
            let calls = request["methodCalls"].as_array().expect("method calls");
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0][0], "Email/set");
            let call_id = calls[0][2].clone();
            Ok(bytes::Bytes::from(
                serde_json::json!({
                    "sessionState": "session-1",
                    "methodResponses": [["error", {"type": "forbidden"}, call_id]]
                })
                .to_string(),
            ))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new(
                "no upload reply",
            ))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new(
                "no download reply",
            ))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new(
                "no session reply",
            ))
        }
    }

    #[derive(Clone)]
    struct SuccessfulSetTransport {
        requests: Arc<StdMutex<Vec<serde_json::Value>>>,
    }

    impl HttpTransport for SuccessfulSetTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
            _replay: bifrost_net::ReplayPolicy,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: serde_json::Value =
                serde_json::from_slice(&body).expect("request is JSON");
            self.requests
                .lock()
                .expect("request log lock")
                .push(request.clone());
            let calls = request["methodCalls"].as_array().expect("method calls");
            assert_eq!(calls.len(), 1);
            assert_eq!(calls[0][0], "Email/set");
            let call_id = calls[0][2].clone();
            Ok(bytes::Bytes::from(
                serde_json::json!({
                    "sessionState": "session-1",
                    "methodResponses": [["Email/set", {
                        "accountId": "primary",
                        "oldState": "state-7",
                        "newState": "state-8",
                        "updated": {"m1": {}}
                    }, call_id]]
                })
                .to_string(),
            ))
        }

        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new(
                "no upload reply",
            ))
        }

        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new(
                "no download reply",
            ))
        }

        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            Err(crate::core::transport::TransportError::new(
                "no session reply",
            ))
        }
    }

    fn mutation_accounts<T: HttpTransport>(
        transport: T,
    ) -> (
        MailAccount<T>,
        Arc<HashMap<crate::core::id::AccountId, MailAccount<T>>>,
    ) {
        let session = serde_json::from_value(serde_json::json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {
                    "maxSizeUpload": 1000,
                    "maxConcurrentUpload": 2,
                    "maxSizeRequest": 100_000,
                    "maxConcurrentRequests": 4,
                    "maxCallsInRequest": 8,
                    "maxObjectsInGet": 256,
                    "maxObjectsInSet": 256,
                    "collationAlgorithms": []
                },
                "urn:ietf:params:jmap:mail": {}
            },
            "accounts": {
                "primary": {"name": "Primary", "isPersonal": true, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}},
                "shared": {"name": "Shared", "isPersonal": false, "isReadOnly": false,
                    "accountCapabilities": {"urn:ietf:params:jmap:mail": {}}}
            },
            "primaryAccounts": {"urn:ietf:params:jmap:mail": "primary"},
            "username": "user@example.test",
            "apiUrl": "https://example.test/jmap/api",
            "downloadUrl": "https://example.test/download/{accountId}/{blobId}/{name}/{type}",
            "uploadUrl": "https://example.test/upload/{accountId}",
            "eventSourceUrl": "https://example.test/eventsource",
            "state": "session-1"
        }))
        .expect("test session parses");
        let client = crate::client::Client::with_transport(
            transport,
            session,
            "https://example.test/.well-known/jmap",
        )
        .expect("client builds");
        let primary = crate::sync::registry::Slot::new(client.clone(), "primary");
        let shared = crate::sync::registry::Slot::new(client, "shared");
        (
            primary,
            Arc::new(HashMap::from([("shared".into(), shared)])),
        )
    }

    #[test]
    fn dotted_flag_patches_and_destroy_omit_account_wide_if_in_state() {
        let add = set_body(
            &MutationKind::Flags(FlagOp::Add(flags(&["$seen"]))),
            &["m1"],
            None,
        );
        let destroy = set_body(&MutationKind::Destroy, &["m1"], None);

        assert!(add.get("ifInState").is_none());
        assert!(destroy.get("ifInState").is_none());
        assert_eq!(destroy["destroy"], serde_json::json!(["m1"]));
    }

    #[test]
    fn whole_property_replacements_keep_if_in_state() {
        let set_flags = set_body(
            &MutationKind::Flags(FlagOp::Set(flags(&["$seen"]))),
            &["m1"],
            Some("state-7"),
        );
        let move_message = set_body(
            &MutationKind::Move(MailboxId::new("mbx-target")),
            &["m1"],
            Some("state-7"),
        );

        assert_eq!(set_flags["ifInState"], serde_json::json!("state-7"));
        assert_eq!(move_message["ifInState"], serde_json::json!("state-7"));
    }

    #[derive(Clone)]
    struct CallerStateTransport {
        accept: bool,
        requests: Arc<StdMutex<Vec<serde_json::Value>>>,
    }

    impl HttpTransport for CallerStateTransport {
        async fn api_request(
            &self,
            _url: &str,
            body: Vec<u8>,
            _replay: bifrost_net::ReplayPolicy,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            let request: serde_json::Value = serde_json::from_slice(&body).unwrap();
            self.requests.lock().unwrap().push(request.clone());
            let call = &request["methodCalls"][0];
            assert_eq!(
                call[0], "Email/set",
                "caller preconditions need no state probe"
            );
            assert_eq!(
                call[1]["ifInState"], "caller-old",
                "never substitute a newer cached state"
            );
            if self.accept {
                return Ok(bytes::Bytes::from(
                    serde_json::json!({
                        "sessionState": "session-1",
                        "methodResponses": [["Email/set", {
                            "accountId": "primary", "oldState": "caller-old", "newState": "next",
                            "updated": {"m1": null, "m2": null}, "destroyed": ["m1", "m2"]
                        }, call[2]]]
                    })
                    .to_string(),
                ));
            }
            Ok(bytes::Bytes::from(
                serde_json::json!({
                    "sessionState": "session-1",
                    "methodResponses": [["error", {"type": "stateMismatch"}, call[2]]]
                })
                .to_string(),
            ))
        }
        async fn upload(
            &self,
            _url: &str,
            _body: Vec<u8>,
            _content_type: Option<&str>,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            unreachable!()
        }
        async fn download(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            unreachable!()
        }
        async fn get_session(
            &self,
            _url: &str,
        ) -> Result<bytes::Bytes, crate::core::transport::TransportError> {
            unreachable!()
        }
    }

    #[tokio::test]
    async fn read_minted_mail_versions_refuse_other_types_and_owners_before_io() {
        use bifrost_types::{ObjectType, ServerVersion};
        for kind in [
            MutationKind::Flags(FlagOp::Add(flags(&["$seen"]))),
            MutationKind::Move(MailboxId::new("archive")),
            MutationKind::Destroy,
        ] {
            for (id, version) in [
                (
                    "m1".to_owned(),
                    ServerVersion::jmap_state("primary", ObjectType::Mailbox, "caller-old"),
                ),
                (
                    "m1".to_owned(),
                    ServerVersion::jmap_state("primary", ObjectType::Contact, "caller-old"),
                ),
                (
                    "m1".to_owned(),
                    ServerVersion::jmap_state("shared", ObjectType::Email, "caller-old"),
                ),
                (
                    super::super::foreign::encode_object("shared", "m1"),
                    ServerVersion::jmap_state("primary", ObjectType::Email, "caller-old"),
                ),
            ] {
                let requests = Arc::new(StdMutex::new(Vec::new()));
                let (primary, foreign) = mutation_accounts(CallerStateTransport {
                    accept: true,
                    requests: Arc::clone(&requests),
                });
                let registry = crate::sync::registry::Registry::from_test_states(
                    primary,
                    foreign,
                    ObjectLimits {
                        max_objects_in_get: std::num::NonZeroUsize::new(256).unwrap(),
                        max_objects_in_set: std::num::NonZeroUsize::new(10).unwrap(),
                    },
                    crate::sync::state_cache::seeded(std::collections::HashMap::new()),
                );
                let events = versioned_stream(
                    Arc::new(registry),
                    Box::pin(futures::stream::iter([bifrost_types::MailMutationTarget {
                        id: ObjectId(id),
                        expected_version: Some(version),
                    }])),
                    kind.clone(),
                )
                .collect::<Vec<_>>()
                .await;
                let [SyncEvent::Batch(batch), SyncEvent::Done(None)] = events.as_slice() else {
                    panic!("{events:?}");
                };
                let [ItemOutcome::Failed(failure)] = batch.items.as_slice() else {
                    panic!("{batch:?}");
                };
                assert_eq!(
                    failure.error.kind(),
                    &bifrost_types::AccountErrorKind::Unsupported(operation_for_kind(&kind))
                );
                assert!(requests.lock().unwrap().is_empty());
            }
        }
    }

    #[tokio::test]
    async fn caller_state_siblings_share_one_guarded_set() {
        for kind in [
            MutationKind::Flags(FlagOp::Add(flags(&["$seen"]))),
            MutationKind::Move(MailboxId::new("archive")),
            MutationKind::Destroy,
        ] {
            for accept in [true, false] {
                let requests = Arc::new(StdMutex::new(Vec::new()));
                let (primary, foreign) = mutation_accounts(CallerStateTransport {
                    accept,
                    requests: Arc::clone(&requests),
                });
                let events = versioned_stream(
                    std::sync::Arc::new(crate::sync::registry::Registry::from_test_states(
                        primary,
                        foreign,
                        ObjectLimits {
                            max_objects_in_get: std::num::NonZeroUsize::new(256).unwrap(),
                            max_objects_in_set: std::num::NonZeroUsize::new(10).unwrap(),
                        },
                        crate::sync::state_cache::seeded(std::collections::HashMap::new()),
                    )),
                    Box::pin(futures::stream::iter(["m1", "m2"].map(|id| {
                        bifrost_types::MailMutationTarget {
                            id: ObjectId(id.into()),
                            expected_version: Some(bifrost_types::ServerVersion::jmap_state(
                                "primary",
                                bifrost_types::ObjectType::Email,
                                "caller-old",
                            )),
                        }
                    }))),
                    kind.clone(),
                )
                .collect::<Vec<_>>()
                .await;
                let [SyncEvent::Batch(batch), SyncEvent::Done(None)] = events.as_slice() else {
                    panic!("{events:?}");
                };
                assert_eq!(batch.items.len(), 2);
                for item in &batch.items {
                    if accept {
                        assert!(matches!(item, ItemOutcome::Succeeded(_)));
                    } else {
                        let ItemOutcome::Failed(failure) = item else {
                            panic!("{item:?}")
                        };
                        assert_eq!(
                            *failure.error.kind(),
                            bifrost_types::AccountErrorKind::ConcurrencyConflict
                        );
                    }
                }
                let calls = requests.lock().unwrap();
                assert_eq!(
                    calls.len(),
                    1,
                    "siblings must not invalidate each other's state"
                );
                let args = &calls[0]["methodCalls"][0][1];
                if matches!(kind, MutationKind::Destroy) {
                    assert_eq!(args["destroy"].as_array().unwrap().len(), 2);
                } else {
                    assert_eq!(args["update"].as_object().unwrap().len(), 2);
                }
            }
        }
    }

    #[tokio::test]
    async fn caller_state_conflicts_once_for_every_bulk_mail_door() {
        for kind in [
            MutationKind::Flags(FlagOp::Add(flags(&["$seen"]))),
            MutationKind::Flags(FlagOp::Set(flags(&["$seen"]))),
            MutationKind::Move(MailboxId::new("archive")),
            MutationKind::Destroy,
        ] {
            let requests = Arc::new(StdMutex::new(Vec::new()));
            let (primary, foreign) = mutation_accounts(CallerStateTransport {
                accept: false,
                requests: Arc::clone(&requests),
            });
            let states = crate::sync::state_cache::seeded(HashMap::from([(
                "primary".into(),
                Some("cache-new".into()),
            )]));
            let events = versioned_stream(
                std::sync::Arc::new(crate::sync::registry::Registry::from_test_states(
                    primary,
                    foreign,
                    ObjectLimits {
                        max_objects_in_get: std::num::NonZeroUsize::new(256).unwrap(),
                        max_objects_in_set: std::num::NonZeroUsize::new(10).unwrap(),
                    },
                    states,
                )),
                Box::pin(futures::stream::iter([bifrost_types::MailMutationTarget {
                    id: ObjectId("m1".into()),
                    expected_version: Some(bifrost_types::ServerVersion::jmap_state(
                        "primary",
                        bifrost_types::ObjectType::Email,
                        "caller-old",
                    )),
                }])),
                kind,
            )
            .collect::<Vec<_>>()
            .await;
            let [SyncEvent::Batch(batch), SyncEvent::Done(None)] = events.as_slice() else {
                panic!("{events:?}");
            };
            let [ItemOutcome::Failed(failure)] = batch.items.as_slice() else {
                panic!("{batch:?}");
            };
            assert_eq!(
                failure.error.kind(),
                &bifrost_types::AccountErrorKind::ConcurrencyConflict
            );
            assert_eq!(
                requests.lock().unwrap().len(),
                1,
                "a caller mismatch must never refresh and retry"
            );
        }
    }

    #[tokio::test]
    async fn bulk_keyword_patch_avoids_state_probe_and_if_in_state() {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let (primary, foreign) = mutation_accounts(SuccessfulSetTransport {
            requests: Arc::clone(&requests),
        });
        let states = crate::sync::state_cache::seeded(HashMap::from([(
            "primary".to_string(),
            Some("state-7".into()),
        )]));
        let input: AccountStream<ObjectId> =
            Box::pin(futures::stream::iter(vec![ObjectId("m1".to_string())]));

        let events = mutation_stream(
            std::sync::Arc::new(crate::sync::registry::Registry::from_test_states(
                primary,
                foreign,
                ObjectLimits {
                    max_objects_in_get: std::num::NonZeroUsize::new(256).unwrap(),
                    max_objects_in_set: std::num::NonZeroUsize::new(10).unwrap(),
                },
                states,
            )),
            input,
            MutationKind::Flags(FlagOp::Add(flags(&["$seen"]))),
        )
        .collect::<Vec<_>>()
        .await;

        assert!(matches!(
            events.as_slice(),
            [SyncEvent::Batch(_), SyncEvent::Done(_)]
        ));
        let requests = requests.lock().expect("request log lock");
        assert_eq!(requests.len(), 1, "a cached Email state needs no probe");
        let call = &requests[0]["methodCalls"][0];
        assert_eq!(call[0], "Email/set");
        let arguments = &call[1];
        assert!(arguments.get("ifInState").is_none());
        assert_eq!(
            arguments["update"]["m1"]["keywords/$seen"],
            serde_json::json!(true)
        );
    }

    #[tokio::test]
    async fn absent_bulk_caller_state_keeps_the_unconditional_keyword_path() {
        let requests = Arc::new(StdMutex::new(Vec::new()));
        let (primary, foreign) = mutation_accounts(SuccessfulSetTransport {
            requests: Arc::clone(&requests),
        });
        let events = versioned_stream(
            std::sync::Arc::new(crate::sync::registry::Registry::from_test_states(
                primary,
                foreign,
                ObjectLimits {
                    max_objects_in_get: std::num::NonZeroUsize::new(256).unwrap(),
                    max_objects_in_set: std::num::NonZeroUsize::new(10).unwrap(),
                },
                crate::sync::state_cache::seeded(std::collections::HashMap::new()),
            )),
            Box::pin(futures::stream::iter([bifrost_types::MailMutationTarget {
                id: ObjectId("m1".into()),
                expected_version: None,
            }])),
            MutationKind::Flags(FlagOp::Add(flags(&["$seen"]))),
        )
        .collect::<Vec<_>>()
        .await;
        let [SyncEvent::Batch(batch), SyncEvent::Done(None)] = events.as_slice() else {
            panic!("{events:?}");
        };
        assert!(matches!(
            batch.items.as_slice(),
            [ItemOutcome::Succeeded(_)]
        ));
        let calls = requests.lock().unwrap();
        assert_eq!(
            calls.len(),
            1,
            "absent state on a keyword patch needs no probe"
        );
        assert!(calls[0]["methodCalls"][0][1].get("ifInState").is_none());
    }

    #[test]
    fn flag_add_and_remove_use_dotted_keyword_paths() {
        // RFC 8620 s5.3: a PatchObject removes a key with `null`, never
        // with `false`. Add sets `true`, remove sets `null`, and both ride
        // dotted `keywords/<flag>` paths so unrelated keywords survive.
        let add = set_body(
            &MutationKind::Flags(FlagOp::Add(flags(&["$seen"]))),
            &["m1"],
            None,
        );
        assert_eq!(
            add["update"]["m1"]["keywords/$seen"],
            serde_json::json!(true)
        );

        let remove = set_body(
            &MutationKind::Flags(FlagOp::Remove(flags(&["$seen"]))),
            &["m1"],
            None,
        );
        assert_eq!(
            remove["update"]["m1"].get("keywords/$seen"),
            Some(&serde_json::Value::Null),
            "removal must be an explicit null key, not false and not an omission"
        );
        // Neither form assigns the whole `keywords` map.
        assert!(add["update"]["m1"].get("keywords").is_none());
        assert!(remove["update"]["m1"].get("keywords").is_none());
    }

    #[test]
    fn flag_set_replaces_the_whole_keyword_map() {
        let body = set_body(
            &MutationKind::Flags(FlagOp::Set(flags(&["$seen"]))),
            &["m1"],
            Some("s1"),
        );
        assert_eq!(
            body["update"]["m1"]["keywords"],
            serde_json::json!({"$seen": true}),
            "FlagOp::Set is a wholesale replacement, so it assigns the map"
        );
        assert!(body["update"]["m1"].get("keywords/$seen").is_none());
    }

    #[test]
    fn a_flag_patch_applies_removals_after_additions() {
        // `FlagOp::Patch` writes adds then removes into one dotted patch.
        // A flag named in both sets therefore ends up removed - pinning the
        // precedence so a reordering does not silently invert it.
        let body = set_body(
            &MutationKind::Flags(FlagOp::Patch {
                add: flags(&["$seen", "$flagged"]),
                remove: flags(&["$seen"]),
            }),
            &["m1"],
            None,
        );
        assert_eq!(
            body["update"]["m1"]["keywords/$flagged"],
            serde_json::json!(true)
        );
        assert_eq!(
            body["update"]["m1"].get("keywords/$seen"),
            Some(&serde_json::Value::Null),
            "remove is applied last and wins"
        );
    }

    // The former `an_empty_flag_op_short_circuits_to_skipped_outcomes` test is
    // deliberately gone with the `SkipFlags` path it pinned: an empty additive
    // or subtractive flag operation is now a rejected caller error rather than
    // a batch of `Skipped` outcomes. The replacement coverage is
    // `FlagOp::validate` in `bifrost-types` and
    // `an_empty_flag_op_is_rejected_before_wire_or_target_routing` in
    // `sync::factory`, which drives the whole account surface.

    #[test]
    fn a_move_assigns_the_destination_as_the_only_mailbox() {
        // JMAP `bulk_move` is a move, not a copy: assigning the whole
        // `mailboxIds` map (rather than dotted add/remove paths) is what
        // drops the source membership.
        let body = set_body(
            &MutationKind::Move(MailboxId::new("mbx-target")),
            &["m1"],
            Some("s1"),
        );
        assert_eq!(
            body["update"]["m1"]["mailboxIds"],
            serde_json::json!({"mbx-target": true})
        );
    }

    #[test]
    fn a_multi_id_batch_carries_one_update_entry_per_id() {
        let body = set_body(
            &MutationKind::Flags(FlagOp::Add(flags(&["$seen"]))),
            &["m1", "m2", "m3"],
            None,
        );
        let update = body["update"].as_object().expect("update is an object");
        assert_eq!(update.len(), 3);
        for id in ["m1", "m2", "m3"] {
            assert_eq!(update[id]["keywords/$seen"], serde_json::json!(true));
        }
    }

    #[test]
    fn surviving_state_mismatch_routes_to_per_item_failed_concurrency_conflict() {
        // A second method-level `stateMismatch` must NOT terminate the
        // bulk stream; every id is emitted on the per-item `Failed` lane
        // classified `ConcurrencyConflict -> Retry::AfterStateRefresh`.
        let err = crate::Error::Method(state_mismatch_method_error());
        let ids = vec![ObjectId("m1".into()), ObjectId("m2".into())];
        let batch =
            state_mismatch_failed_batch(err, &MutationKind::Destroy, ids, Instant::now(), 0);

        assert_eq!(batch.items.len(), 2);
        for (idx, item) in batch.items.iter().enumerate() {
            match item {
                ItemOutcome::Failed(failure) => {
                    assert_eq!(failure.error.kind(), &AccountErrorKind::ConcurrencyConflict);
                    match failure.error.recovery() {
                        RecoveryClass::Retry(advice) => {
                            assert_eq!(advice.disposition, RetryDisposition::AfterStateRefresh);
                        }
                        other => panic!("expected Retry::AfterStateRefresh, got {other:?}"),
                    }
                    // Each lane entry is scoped to its own message id.
                    let expected = if idx == 0 { "m1" } else { "m2" };
                    assert_eq!(failure.item.0, expected);
                }
                other => panic!("expected Failed, got {other:?}"),
            }
        }
    }

    /// Route membership is per OWNER, not per input target. Draining used
    /// to remove the map entry, so the next target for the same owner
    /// re-registered it and appended another `String`; at
    /// `maxObjectsInSet = 1` that is one allocation retained per input item
    /// for the whole run.
    #[test]
    fn a_repeatedly_drained_route_is_registered_exactly_once() {
        let mut buffers = RouteBuffers::new(1);
        for n in 0..64 {
            let due = buffers.push("acct-9".to_string(), ObjectId(format!("m{n}")), None);
            assert_eq!(due, vec!["acct-9".to_string()]);
            assert!(buffers.drain("acct-9").is_some());
        }
        assert_eq!(buffers.remaining(), vec!["acct-9".to_string()]);
    }

    /// A short batch cannot be held hostage by another owner's traffic. The
    /// quiet owner is flushed once `batch_size` further targets have gone
    /// elsewhere - the same wait a full batch already accepts - instead of
    /// waiting for the input stream to end.
    #[test]
    fn a_partial_batch_flushes_once_a_full_batch_of_input_has_passed_it() {
        let mut buffers = RouteBuffers::new(4);
        assert!(
            buffers
                .push("quiet".to_string(), ObjectId("q1".to_string()), None)
                .is_empty()
        );
        for n in 0..3 {
            assert!(
                buffers
                    .push("busy".to_string(), ObjectId(format!("b{n}")), None)
                    .is_empty(),
                "nothing is due while both routes are short and freshly fed"
            );
        }

        // The fourth `busy` target both fills that route and ages `quiet`
        // by a full batch worth of input.
        let due = buffers.push("busy".to_string(), ObjectId("b3".to_string()), None);
        assert_eq!(due, vec!["quiet".to_string(), "busy".to_string()]);
        assert_eq!(
            buffers.drain("quiet").expect("quiet route").0,
            vec![ObjectId("q1".to_string())]
        );
    }

    /// An emptied route is never re-flushed by the idle sweep, and the
    /// end-of-input drain skips it too.
    #[test]
    fn an_emptied_route_is_not_flushed_again() {
        let mut buffers = RouteBuffers::new(1);
        buffers.push("acct-9".to_string(), ObjectId("m1".to_string()), None);
        assert!(buffers.drain("acct-9").is_some());
        let due = buffers.push("primary".to_string(), ObjectId("m2".to_string()), None);
        assert_eq!(due, vec!["primary".to_string()]);
        assert!(buffers.drain("acct-9").is_none());
    }

    /// A locally-rejected target still occupies a slot in its owner's
    /// batch, so a stream of nothing but rejections cannot accumulate
    /// without bound waiting for a wire flush that never comes.
    #[test]
    fn a_rejected_target_counts_toward_its_owners_batch() {
        let rejected = ItemOutcome::Failed(BatchFailure::new(
            BatchItemId("m1".to_string()),
            super::super::error::cross_account_destination(
                AccountOperation::BulkMove,
                "m1",
                None,
                "acct-9\u{1f}mbx",
                Some("acct-9"),
            ),
        ));
        let mut buffers = RouteBuffers::new(1);
        let due = buffers.push(
            "primary".to_string(),
            ObjectId("m1".to_string()),
            Some(rejected),
        );
        assert_eq!(due, vec!["primary".to_string()]);
        let (wire, rejected) = buffers.drain("primary").expect("primary route");
        assert!(wire.is_empty(), "a rejected target never reaches the wire");
        assert_eq!(rejected.len(), 1);
    }

    #[tokio::test]
    async fn locally_rejected_targets_are_emitted_before_a_wire_termination() {
        let (primary, foreign) = mutation_accounts(FailingSetTransport);
        let foreign_id = ObjectId(super::super::foreign::encode_object("shared", "M9"));
        let mut states = HashMap::new();
        states.insert("primary".to_string(), Some("primary-state".into()));
        states.insert("shared".to_string(), Some("shared-state".into()));
        let states = crate::sync::state_cache::seeded(states);
        let input: AccountStream<ObjectId> = Box::pin(futures::stream::iter(vec![
            ObjectId("m1".to_string()),
            ObjectId("m2".to_string()),
            foreign_id.clone(),
            ObjectId("m3".to_string()),
        ]));

        let events = mutation_stream(
            std::sync::Arc::new(crate::sync::registry::Registry::from_test_states(
                primary,
                foreign,
                ObjectLimits {
                    max_objects_in_get: std::num::NonZeroUsize::new(256).unwrap(),
                    max_objects_in_set: std::num::NonZeroUsize::new(3).unwrap(),
                },
                states,
            )),
            input,
            MutationKind::Move(MailboxId::new("primary-inbox")),
        )
        .collect::<Vec<_>>()
        .await;

        assert_eq!(events.len(), 2);
        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("locally decided outcomes precede stream termination");
        };
        assert_eq!(batch.items.len(), 1);
        let ItemOutcome::Failed(failure) = &batch.items[0] else {
            panic!("the cross-account destination is a local failure");
        };
        assert_eq!(failure.item.0.as_str(), foreign_id.0.as_str());
        assert!(matches!(&events[1], SyncEvent::Terminated(_)));
    }

    #[test]
    fn draining_rejections_preserves_owner_order_and_wire_targets() {
        let mut buffers = RouteBuffers::new(10);
        let destination = MutationKind::Move(MailboxId::new("primary-inbox"));
        for (owner, native) in [("z-owner", "m1"), ("a-owner", "m2"), ("z-owner", "m3")] {
            let target = ObjectId(super::super::foreign::encode_object(owner, native));
            let rejection = destination_rejection(&destination, &target);
            assert!(rejection.is_some());
            buffers.push(owner.to_string(), target, rejection);
        }
        let wire = ObjectId("wire-target".to_string());
        buffers.push("z-owner".to_string(), wire.clone(), None);

        let rejected = buffers.drain_rejections();
        let ids: Vec<&str> = rejected
            .iter()
            .map(|outcome| {
                let ItemOutcome::Failed(failure) = outcome else {
                    panic!("expected local rejection");
                };
                failure.item.0.as_str()
            })
            .collect();
        let expected = [
            super::super::foreign::encode_object("z-owner", "m1"),
            super::super::foreign::encode_object("z-owner", "m3"),
            super::super::foreign::encode_object("a-owner", "m2"),
        ];
        assert_eq!(ids, expected.iter().map(String::as_str).collect::<Vec<_>>());
        assert!(buffers.drain_rejections().is_empty());
        let (items, rejected) = buffers.drain("z-owner").expect("wire target remains");
        assert_eq!(items, vec![wire]);
        assert!(rejected.is_empty());
        assert!(buffers.drain("a-owner").is_none());
    }

    /// The owner comparison behind the wire-level rejection, enumerated.
    /// Both operands bare, or both qualified with the same account, is the
    /// only expressible pairing.
    #[test]
    fn a_move_destination_must_name_the_targets_own_account() {
        let foreign_target = ObjectId(super::super::foreign::encode_object("acct-9", "M1"));
        let primary_target = ObjectId("m1".to_string());
        let foreign_box = |account: &str| {
            MutationKind::Move(MailboxId::new(
                super::super::foreign::encode_foreign(account, "mbx").0,
            ))
        };
        let primary_box = MutationKind::Move(MailboxId::new("inbox"));

        assert!(destination_rejection(&primary_box, &primary_target).is_none());
        assert!(destination_rejection(&foreign_box("acct-9"), &foreign_target).is_none());

        // The silent one: a bare primary mailbox id would otherwise be
        // resolved inside acct-9's namespace.
        let rejected = destination_rejection(&primary_box, &foreign_target)
            .expect("a primary destination cannot receive a foreign message");
        let ItemOutcome::Failed(failure) = rejected else {
            panic!("a cross-account move must land on the item Failed lane");
        };
        assert_eq!(failure.item.0, foreign_target.0);
        assert_eq!(
            failure.error.kind(),
            &AccountErrorKind::Request(bifrost_types::RequestErrorKind::Malformed)
        );
        assert!(matches!(failure.error.recovery(), RecoveryClass::ClientBug));

        assert!(destination_rejection(&foreign_box("acct-9"), &primary_target).is_some());
        assert!(destination_rejection(&foreign_box("acct-7"), &foreign_target).is_some());

        // Kinds with no destination never reject.
        assert!(destination_rejection(&MutationKind::Destroy, &foreign_target).is_none());
        assert!(
            destination_rejection(
                &MutationKind::Flags(FlagOp::Add(flags(&["$seen"]))),
                &foreign_target,
            )
            .is_none()
        );
    }

    #[test]
    fn an_unanswered_set_id_is_a_reconcilable_partial_response() {
        let outcome = classify_set_response(
            Err(crate::Error::IdUnanswered {
                id: "m1".into(),
                context: crate::core::answer::AnswerContext {
                    method: "Email/set",
                    guarded: false,
                    write: true,
                },
            }),
            ObjectId("m1".to_string()),
            AccountOperation::BulkDestroy,
        );

        let ItemOutcome::Failed(failure) = outcome else {
            panic!("an omitted set answer must fail on the item lane");
        };
        assert_eq!(failure.item.0, "m1");
        assert_eq!(
            failure.error.kind(),
            &AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::PartialResponse)
        );
        assert!(failure.error.recovery().requires_reconciliation());
        assert_eq!(
            failure.error.telemetry_fields().transmission_state,
            Some(bifrost_types::TransmissionState::Acknowledged)
        );
    }
    fn replay_transport(
        replies: Vec<bifrost_net::test_support::Canned>,
    ) -> (
        crate::transport_reqwest::ReqwestTransport,
        Arc<bifrost_net::test_support::ScriptedDispatch>,
    ) {
        let script = bifrost_net::test_support::ScriptedDispatch::new(replies);
        let mut policy = bifrost_net::RetryPolicy::default();
        policy.max_attempts = 2;
        policy.initial_backoff = std::time::Duration::ZERO;
        policy.max_backoff = std::time::Duration::ZERO;
        (
            crate::transport_reqwest::ReqwestTransport::scripted_with_retry(&script, policy),
            script,
        )
    }
    #[tokio::test]
    async fn a_503_before_state_mismatch_reconciles_without_replay() {
        use bifrost_net::test_support::canned;
        for ambiguous in [true, false] {
            let mut replies = Vec::new();
            if ambiguous {
                replies.push(canned(
                    reqwest::StatusCode::SERVICE_UNAVAILABLE,
                    b"ambiguous",
                ));
            }
            replies.push(canned(reqwest::StatusCode::OK, br#"{"sessionState":"session-1","methodResponses":[["error",{"type":"stateMismatch"},"s0"]]}"#));
            let (transport, script) = replay_transport(replies);
            let (mail, _) = mutation_accounts(transport);
            let result = apply_batch_with_version(
                &mail,
                &MutationKind::Flags(FlagOp::Add(flags(&["$seen"]))),
                &mut vec![ObjectId("m1".into())],
                Some("caller-old"),
            )
            .await;
            if ambiguous {
                let error = super::super::error::into_account_error(
                    result.err().unwrap(),
                    super::super::error::JmapErrorContext::new(AccountOperation::UpdateFlags),
                );
                assert_eq!(
                    error.kind(),
                    &AccountErrorKind::Protocol(bifrost_types::ProtocolErrorKind::PartialResponse)
                );
                assert!(
                    matches!(error.recovery(), bifrost_types::RecoveryClass::Reconcile(a) if a.guidance.actions == [bifrost_types::ReconcileAction::CheckTarget])
                );
                assert_eq!(script.remaining(), 1);
            } else {
                let batch = result.unwrap().unwrap();
                assert!(
                    matches!(&batch.items[0], ItemOutcome::Failed(f) if f.error.kind() == &AccountErrorKind::ConcurrencyConflict)
                );
            }
            assert_eq!(script.requests().len(), 1);
            let requests = script.requests();
            let body: serde_json::Value =
                serde_json::from_slice(requests[0].body.as_ref().unwrap()).unwrap();
            assert_eq!(body["methodCalls"][0][0], "Email/set");
            assert_eq!(body["methodCalls"][0][1]["ifInState"], "caller-old");
        }
    }
    #[tokio::test]
    async fn a_guarded_flag_write_with_an_omitted_id_reconciles() {
        let (transport, script) = replay_transport(vec![bifrost_net::test_support::canned(reqwest::StatusCode::OK, br#"{"sessionState":"session-1","methodResponses":[["Email/set",{"accountId":"primary","oldState":"caller-old","newState":"new"},"s0"]]}"#)]);
        let (mail, _) = mutation_accounts(transport);
        let batch = apply_batch_with_version(
            &mail,
            &MutationKind::Flags(FlagOp::Add(flags(&["$seen"]))),
            &mut vec![ObjectId("m1".into())],
            Some("caller-old"),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(
            matches!(&batch.items[0], ItemOutcome::Failed(f) if matches!(f.error.recovery(), bifrost_types::RecoveryClass::Reconcile(a) if a.guidance.actions == [bifrost_types::ReconcileAction::CheckTarget]))
        );
        let requests = script.requests();
        let body: serde_json::Value =
            serde_json::from_slice(requests[0].body.as_ref().unwrap()).unwrap();
        assert_eq!(body["methodCalls"][0][1]["ifInState"], "caller-old");
    }
}
