use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use bifrost_types::{
    AccountErrorKind, AccountOperation, AccountStream, Batch, BatchFailure, BatchItemId,
    BatchSuccess, Checkpoint, CursorScope, Fingerprint, HydratedObject, HydratedObjectKind,
    InventoryEntry, ItemOutcome, LabelId, MembershipScope, ObjectId, PageBoundary, Projection,
    ResourceKind, ServerVersion, SyncEvent, ThreadId,
};
use bytes::Bytes;
use futures::{StreamExt, stream};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

use crate::client::GmailClient;
use crate::encoding::decode_base64url_nopad;
use crate::error::PageRefusal;
use crate::headers::find_header_value_case_insensitive;
#[cfg(test)]
use crate::types::GmailLabel;
use crate::types::{GmailHeader, GmailMessage, GmailProfile};

use super::blobs;
use super::cursor::cursor_for_history;
use super::error;
use super::flags;
use super::scopes::{ScopeCache, labels_for_flags};

const LIST_PAGE_SIZE: u32 = 500;
const HYDRATE_BATCH_SIZE: usize = 32;
const MAX_INVENTORY_PAGES: usize = 10_000;

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ListMessagesResponse {
    #[serde(default)]
    messages: Vec<GmailMessageStub>,
    next_page_token: Option<String>,
}

#[derive(Debug, Deserialize)]
struct GmailMessageStub {
    id: String,
}

/// Coverage for a checkpoint emitted after `obligations` were observed.
///
/// Every checkpoint from the first unresolved obligation onward declares the
/// gap, not just the terminal one: a checkpoint certifies the results BEFORE
/// it, and a page checkpoint that claimed `Complete` would let the cursor
/// advance past an object nothing recorded.
fn coverage_of(
    scope: &CursorScope,
    obligations: &[bifrost_types::InventoryObligation],
) -> bifrost_types::InventoryCoverageReport {
    // `Full` is the honest domain: Gmail inventory walks the whole mailbox in
    // one pass rather than by partition, so a clean finish here really does
    // prove the whole scope.
    bifrost_types::InventoryCoverageReport::from_obligations(
        bifrost_types::CoverageDomain::full(scope.clone()),
        obligations,
    )
}

/// Work off inventory coverage debt by re-reading each object from Gmail.
///
/// Gmail's object lane is the simple case: a message is addressable by id
/// alone, so the repair token is empty and the request needs nothing the walk
/// did not already know. `users.messages.get` is authoritative for current
/// existence, and the entry is rebuilt from that fresh read rather than from
/// anything cached - which is what makes the recovered representation safe to
/// announce.
///
/// One terminal outcome per request, always, including for shapes this account
/// cannot serve: a region request reaches here only through an engine bug, and
/// answering it with `Deferred` keeps the correlation contract intact instead
/// of leaving the attempt dangling.
pub(crate) fn repair_inventory(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    requests: AccountStream<bifrost_types::InventoryRepairRequest>,
) -> AccountStream<bifrost_types::InventoryRepairEvent> {
    // This Account operation receives no shutdown token. A token created here
    // would never observe close(); the account adapter must wrap this stream
    // with its token or pass that token through to interrupt a refresh.
    Box::pin(async_stream::stream! {
        let labels = match labels_for_flags(&client, &cache).await {
            Ok(labels) => Arc::new(labels),
            Err(error) => {
                // No per-request conclusion is possible without the label map,
                // so the stream terminates and the engine converts every
                // outstanding attempt to a local deferral. It must not invent
                // an answer none of them received. The failing call is
                // `labels.list`, so the error names that account-wide
                // resource - not the message resource and cursor scope the
                // inventory WALK carries, which would classify a refusal as
                // a missing message and hand the engine a cursor coordinate
                // for a fault that has nothing to do with the cursor.
                yield bifrost_types::InventoryRepairEvent::Terminated(
                    error::into_account_error(error, error::GmailErrorContext::repair_labels()),
                );
                return;
            }
        };
        let label_names = Arc::new(flags::label_name_index(labels.as_slice()));

        let mut requests = requests;
        while let Some(request) = requests.next().await {
            let attempt = request.attempt;
            let bifrost_types::InventoryRepairTarget::Object { id, .. } = &request.target else {
                // Deliberately still `inventory()` (resource `Message`,
                // scope `Cursor(Account)`), unlike the label-refresh
                // terminators above and in `get_stream`, which moved to
                // `repair_labels()` / `hydrate_labels()`: this refusal IS
                // about the inventory cursor scope - Gmail has no region
                // lane - so the cursor coordinate is the truthful one.
                let account_error = error::into_account_error(
                    crate::error::Error::unsupported(AccountOperation::SyncInventory),
                    error::GmailErrorContext::inventory(),
                );
                yield bifrost_types::InventoryRepairEvent::Outcome(
                    bifrost_types::InventoryRepairOutcome::Deferred { attempt, error: account_error },
                );
                continue;
            };

            let outcome = match client.get_message(&id.0, "metadata").await {
                Ok(message) => {
                    // Rebuilding the entry IS the proof: the obligation was
                    // raised because this object could not be represented, so
                    // re-reading it is only half the answer.
                    let entry = inventory_entry_from_message_indexed(
                        &message,
                        &label_names,
                    );
                    bifrost_types::InventoryRepairOutcome::ObjectRecovered {
                        attempt,
                        entry: Box::new(entry),
                    }
                }
                Err(error) => {
                    let account_error = error::into_account_error(
                        error,
                        error::GmailErrorContext::inventory(),
                    );
                    if matches!(
                        account_error.kind(),
                        AccountErrorKind::NotFound(ResourceKind::Message)
                    ) {
                        // Definitive, and the reason is the cursor model rather
                        // than the status code. Gmail's cursor is anchored at
                        // the historyId sampled BEFORE the walk that raised
                        // this obligation, so any deletion since then is
                        // reported by the change stream the consumer is already
                        // reading. Absence is therefore the correct inventory
                        // state and nothing is owed. Under a different
                        // pagination or cursor model the same NotFound would
                        // NOT be dischargeable.
                        bifrost_types::InventoryRepairOutcome::DefinitivelyIrrelevant {
                            attempt,
                            evidence:
                                bifrost_types::DefinitiveIrrelevance::AbsentUnderCursorBridge {
                                    detail: format!(
                                        "gmail message {} absent; the scope cursor is anchored \
                                         before the walk that raised this obligation, so its \
                                         removal is carried by the change stream",
                                        id.0
                                    ),
                                },
                        }
                    } else {
                        bifrost_types::InventoryRepairOutcome::Deferred {
                            attempt,
                            error: account_error,
                        }
                    }
                }
            };
            yield bifrost_types::InventoryRepairEvent::Outcome(outcome);
        }
    })
}

#[cfg(test)]
pub(crate) fn inventory_stream(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    scope: CursorScope,
) -> AccountStream<bifrost_types::InventoryEvent> {
    inventory_stream_cancellable(client, cache, scope, CancellationToken::new())
}

/// Two properties here are load-bearing and were each paid for; do not relax
/// either while "simplifying" the walk.
///
/// A refused or cancelled walk claims NOTHING. Every exit short of a genuine
/// completion emits only `Terminated` - never `Done`, never a final page
/// boundary, never a checkpoint, never a coverage report - so a truncated
/// enumeration can never advance a cursor past objects it did not list. The
/// page budget and repeated-token guard below run BEFORE hydration or emission
/// of the page in hand, for exactly this reason.
///
/// Accepted residual: there is no durable mid-walk resume token, so
/// cancellation restarts the pass and the hydrated items of an unfinished page
/// are discarded with it. That is wasted work, never wrong work - the
/// alternative is a positional resume coordinate over a list Gmail does not
/// promise is stable, which trades cheap repetition for silent skips.
pub(crate) fn inventory_stream_cancellable(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    scope: CursorScope,
    shutdown: CancellationToken,
) -> AccountStream<bifrost_types::InventoryEvent> {
    if !matches!(scope, CursorScope::Account) {
        let account_error = error::into_account_error(
            crate::error::Error::unsupported(bifrost_types::AccountOperation::SyncInventory),
            error::GmailErrorContext::inventory(),
        );
        return Box::pin(stream::iter([bifrost_types::InventoryEvent::Terminated(
            account_error,
        )]));
    }

    Box::pin(async_stream::stream! {
        if shutdown.is_cancelled() {
            return;
        }
        // Every request this walk makes - prelude, each list page, and
        // the concurrent hydration fan-out - reports into one
        // accumulator, and each emitted batch takes and clears it. The
        // fan-out is exactly why the accumulator is needed: a single
        // batch covers HYDRATE_BATCH_SIZE concurrent GETs plus its list
        // page, so no per-response value could stand in for it.
        let (client, tally) = client.metered();
        let client = Arc::new(client);
        // Both preludes must complete before the first list page.
        //
        // The checkpoint anchors to the historyId sampled *before* the
        // walk, not after it. `users.messages.list` returns newest-first,
        // so a message arriving mid-walk sorts ahead of every page still
        // to come and appears in none of them; anchoring to the id
        // observed at the end would put that message in neither the
        // inventory nor the subsequent change stream. Anchoring early
        // replays a few rows instead, which is idempotent for the engine.
        let prelude = async {
            let profile = client.get_profile().await?;
            let checkpoint = inventory_checkpoint(&profile)?;
            let labels = labels_for_flags(&client, &cache).await?;
            Ok::<_, crate::Error>((checkpoint, Arc::new(labels)))
        };
        let prelude = tokio::select! {
            () = shutdown.cancelled() => return,
            result = prelude => result,
        };
        let (checkpoint, labels) = match prelude {
            Ok(prelude) => prelude,
            Err(error) => {
                let account_error = error::into_account_error(
                    error,
                    error::GmailErrorContext::inventory(),
                );
                yield bifrost_types::InventoryEvent::Terminated(account_error);
                return;
            }
        };
        let label_names = Arc::new(flags::label_name_index(labels.as_slice()));
        let mut page_token = None;
        let mut pages_walked = 0;
        let mut seen_page_tokens = HashSet::new();
        // Objects this walk discovered but could not represent. Once non-empty
        // every later checkpoint declares DEGRADED coverage, because a
        // checkpoint must name every unresolved obligation preceding it.
        let mut obligations: Vec<bifrost_types::InventoryObligation> = Vec::new();

        loop {
            let started = Instant::now();
            let page_result = tokio::select! {
                () = shutdown.cancelled() => return,
                result = list_messages_page(&client, page_token.as_deref()) => result,
            };
            let page = match page_result {
                Ok(page) => page,
                Err(error) => {
                    let account_error = error::into_account_error(
                        error,
                        error::GmailErrorContext::inventory(),
                    );
                    yield bifrost_types::InventoryEvent::Terminated(account_error);
                    return;
                }
            };

            pages_walked += 1;
            if let Some(refusal) = inventory_walk_refusal(
                &mut seen_page_tokens,
                pages_walked,
                page.next_page_token.as_deref(),
            ) {
                // Discard this page. No checkpoint has been emitted, so a
                // retry starts from the pre-walk history id and no partial
                // enumeration can be mistaken for complete coverage.
                let account_error = error::into_account_error(
                    crate::error::Error::from(refusal),
                    error::GmailErrorContext::inventory(),
                );
                yield bifrost_types::InventoryEvent::Terminated(account_error);
                return;
            }

            let final_page = page.next_page_token.is_none();
            // The id travels with its result: a per-item failure has to NAME
            // the object it could not represent, or the obligation is a region
            // with nothing to retry against.
            let mut hydrated = stream::iter(page.messages.into_iter().map(|stub| {
                let client = Arc::clone(&client);
                let label_names = Arc::clone(&label_names);
                async move {
                    let outcome = client
                        .get_message(&stub.id, "metadata")
                        .await
                        .map(|message| inventory_entry_from_message_indexed(
                            &message,
                            &label_names,
                        ));
                    (stub.id, outcome)
                }
            }))
            .buffer_unordered(HYDRATE_BATCH_SIZE);

            let mut items = Vec::with_capacity(HYDRATE_BATCH_SIZE);
            loop {
                let next = tokio::select! {
                    () = shutdown.cancelled() => return,
                    next = hydrated.next() => next,
                };
                let Some((id, result)) = next else { break; };
                match result {
                    Ok(item) => {
                        items.push(item);
                    }
                    Err(error) => {
                        let account_error = error::into_account_error(
                            error,
                            error::GmailErrorContext::inventory(),
                        );
                        // A message may be deleted after users.messages.list
                        // names it and before users.messages.get hydrates it.
                        // That is DISCHARGED, not deferred: the id came from
                        // this walk's own listing, the cursor is anchored
                        // before the walk, and absence is the correct
                        // inventory state - so nothing is owed. Note this
                        // rests on those facts, not on the error kind alone;
                        // the same NotFound under a different pagination model
                        // would not be dischargeable.
                        if matches!(
                            account_error.kind(),
                            AccountErrorKind::NotFound(ResourceKind::Message)
                        ) {
                            continue;
                        }
                        // Everything else is an OBLIGATION, not a reason to
                        // discard the walk. Terminating here cost the whole
                        // backfill partition - every page already emitted -
                        // for one unreadable object, repeatedly, because the
                        // next attempt hit the same object. The walk now
                        // continues and the checkpoint declares the gap, so
                        // the scope converges while staying honest about what
                        // it is missing.
                        obligations.push(bifrost_types::InventoryObligation::Object {
                            // The Gmail message id IS the stable identity, and
                            // it is stable across walks, so the same
                            // unreadable message re-raises the same key rather
                            // than minting a fresh obligation every pass -
                            // which is what would let it evade a retry budget.
                            key: bifrost_types::ObligationKey(
                                format!("gmail:message:{id}").into_bytes(),
                            ),
                            id: bifrost_types::ObjectId(id),
                            error: account_error,
                            // No provider-native repair token: a Gmail message
                            // is re-readable from its id alone.
                            repair: Vec::new(),
                        });
                        continue;
                    }
                }
            }

            if final_page {
                let coverage = coverage_of(&scope, &obligations);
                yield bifrost_types::InventoryEvent::Batch(bifrost_types::InventoryBatch {
                    items,
                    page_boundary: PageBoundary::Final,
                    server_latency: started.elapsed(),
                    bytes_in: tally.take(),
                    checkpoint: bifrost_types::PageCheckpoint::new(checkpoint.clone(), &coverage),
                    coverage: coverage.clone(),
                });
                // The listing ran dry: this is a whole-scope walk at its end.
                yield bifrost_types::InventoryEvent::Done(bifrost_types::InventoryCompletion::new(
                    checkpoint,
                    coverage,
                    bifrost_types::PartitionEnd::Exhausted,
                ));
                break;
            }

            if !items.is_empty() {
                let coverage = coverage_of(&scope, &obligations);
                yield bifrost_types::InventoryEvent::Batch(bifrost_types::InventoryBatch {
                    items,
                    page_boundary: PageBoundary::Page,
                    server_latency: started.elapsed(),
                    bytes_in: tally.take(),
                    checkpoint: bifrost_types::PageCheckpoint::new(None, &coverage),
                    coverage,
                });
            }
            page_token = page.next_page_token;
        }
    })
}

#[cfg(test)]
pub(crate) fn get_stream(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    ids: AccountStream<ObjectId>,
    projection: Projection,
) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
    get_stream_cancellable(client, cache, ids, projection, CancellationToken::new())
}

/// Surveyed against the crate's other emitting streams and deliberately NOT
/// merged into a shared batched-stream driver. Recorded here so the next reader
/// does not redo the survey and reach the opposite conclusion.
///
/// The candidates were this function, `changes::changes_stream_cancellable`,
/// `mutation::mutation_stream`, `scopes::scope_lifecycle_stream`, and
/// `inventory_stream_cancellable` above. Only the first three are
/// `stream::unfold` at all: the lifecycle stream is an unfold but emits a
/// different event enum with no batches, no boundary, no byte tally and no
/// terminal `Done`, and the inventory walk is an `async_stream::stream!`. What
/// the three unfolds actually share is a seven-line `finished` / `emitted_done`
/// epilogue. Everything around it differs, and each difference is load-bearing:
///
/// - Batch source. Mutation and hydration drain an input `AccountStream`; the
///   change stream pages a provider endpoint and has no input stream; the
///   lifecycle stream polls on a timer with backoff.
/// - Lookahead. Mutation reads one item past a full batch so a stream whose
///   length divides the cap still ends `Final`. Hydration must NOT: its ids
///   come from a backpressured producer that may be waiting on hydration
///   output, so a lookahead deadlocks the two sides (see the comment in the
///   drain loop below). One shared drain helper cannot hold both rules without
///   a flag whose two settings are the two loops.
/// - Shutdown. The read streams end silently, publishing nothing. The mutation
///   driver cannot: after dispatch it owes `Uncertain` lanes plus a terminator,
///   which is why it alone drains a pending-event slot BEFORE testing the
///   token, inverting the order every other machine uses.
/// - Failure disposition. Four lanes, four questions, not four answers to one.
///   A mutation batch was transmitted as a set, so the question is whether the
///   refusal is attributable per id (`error::terminates_mutation_stream`). The
///   inventory walk owns a coverage-obligation ledger, so it has a third answer
///   nothing else has: discharge, defer as an obligation, or terminate on a
///   list-page failure. A hydration item named its own id and affects no other,
///   so it always takes a `Failed` lane while a label-vocabulary failure ahead
///   of any transmission terminates. The lifecycle poll has no items at all and
///   asks only retry-or-escalate, which the `RecoveryClass` answers alone. A
///   `FailurePolicy::for(error, lane)` returning `Vec<ItemOutcome>` /
///   `InventoryObligation` / `ItemOutcome` / continue-or-stop is this match
///   re-spelled behind an indirection, and it would move the mutation rule away
///   from the classification in `error.rs` that it reads.
///
/// A driver covering all of it needs roughly a dozen parameters, several of
/// them closures returning the next batch, to delete seven duplicated lines
/// three times. Keep the machines separate.
pub(crate) fn get_stream_cancellable(
    client: Arc<GmailClient>,
    cache: ScopeCache,
    ids: AccountStream<ObjectId>,
    projection: Projection,
    shutdown: CancellationToken,
) -> AccountStream<SyncEvent<ItemOutcome<HydratedObject>>> {
    // Hydration batches are the clearest case for a batch-scoped
    // accumulator: one emitted batch covers up to HYDRATE_BATCH_SIZE
    // `users.messages.get` calls plus any label refresh they needed.
    let (client, tally) = client.metered();
    let client = Arc::new(client);
    let state = HydrateState {
        client,
        tally,
        cache,
        ids,
        projection,
        shutdown,
        finished: false,
        emitted_done: false,
    };

    Box::pin(stream::unfold(state, |mut state| async move {
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

        let mut ids = Vec::new();
        // `final_batch` is set only when the drain itself observes the id
        // stream close. Never poll past a full batch to learn whether more
        // ids follow: `ids` is a backpressured producer that may be waiting
        // on our output before it yields again, so a lookahead poll here
        // deadlocks hydration against its own caller, and even when it does
        // not it holds every batch hostage until one more id arrives. A full
        // last batch therefore stays `Page` and the terminator is the
        // following `Done`; the boundary is advisory and losing it on that
        // one alignment is strictly cheaper than a stall. Accepted rather
        // than open: no `bifrost-sync` hydration path reads `Final`, so the
        // boundary is advisory here, and it would only stop being advisory
        // if the engine started deriving completion from it - at which point
        // the fix is a declared exhaustion signal on the id stream, not a
        // lookahead.
        let mut final_batch = false;
        while ids.len() < HYDRATE_BATCH_SIZE {
            let next = tokio::select! {
                () = state.shutdown.cancelled() => return None,
                next = state.ids.next() => next,
            };
            match next {
                Some(id) => ids.push(id),
                None => {
                    final_batch = true;
                    break;
                }
            }
        }
        if ids.is_empty() {
            state.finished = true;
            state.emitted_done = true;
            return Some((SyncEvent::Done(None), state));
        }
        if final_batch {
            state.finished = true;
        }

        // Resolved after the id drain, not before it: `labels_for_flags`
        // can hit the network, and the poll that discovers an exhausted
        // id stream has no hydration to canonicalize for.
        let labels_result = tokio::select! {
            () = state.shutdown.cancelled() => return None,
            result = labels_for_flags(&state.client, &state.cache) => result,
        };
        let labels = match labels_result {
            Ok(labels) => labels,
            Err(error) => {
                state.finished = true;
                state.emitted_done = true;
                // Not one id of the batch was transmitted: the label
                // refresh failed before any `users.messages.get` went out,
                // so every drained id stays UNREPORTED behind the
                // terminator, which is what `Terminated` has always meant
                // here. The scope is therefore the account-wide label
                // vocabulary rather than an arbitrary first id - naming
                // `ids[0]` pointed support exports at a message the failing
                // request never mentioned, and said nothing about the other
                // 31.
                let account_error =
                    error::into_account_error(error, error::GmailErrorContext::hydrate_labels());
                return Some((SyncEvent::Terminated(account_error), state));
            }
        };

        let started = Instant::now();
        let label_names = Arc::new(flags::label_name_index(&labels));
        let hydrate_client = Arc::clone(&state.client);
        let projection = state.projection;
        let mut hydrated = stream::iter(ids.into_iter().map(move |id| {
            let client = Arc::clone(&hydrate_client);
            let label_names = Arc::clone(&label_names);
            async move {
                // Clone the id so a failing hydrate can attach
                // `ErrorScope::Message { id }` to the resulting
                // `AccountError` (and so the per-item lane carries it as
                // a `BatchItemId`); moved into `hydrate_one`, the error
                // scope would carry an empty string.
                let id_for_error = id.0.clone();
                let result = hydrate_one(&client, &label_names, id, projection).await;
                (id_for_error, result)
            }
        }))
        .buffer_unordered(HYDRATE_BATCH_SIZE);
        let mut items: Vec<ItemOutcome<HydratedObject>> = Vec::new();
        loop {
            let next = tokio::select! {
                () = state.shutdown.cancelled() => return None,
                next = hydrated.next() => next,
            };
            let Some((id_for_error, result)) = next else {
                break;
            };
            match result {
                Ok(hydrated) => {
                    items.push(ItemOutcome::Succeeded(BatchSuccess::new(
                        BatchItemId(id_for_error),
                        hydrated,
                    )));
                }
                Err(err) => {
                    let account_error = error::into_account_error(
                        err,
                        error::GmailErrorContext::hydrate_message(id_for_error.clone()),
                    );
                    items.push(ItemOutcome::Failed(BatchFailure::new(
                        BatchItemId(id_for_error),
                        account_error,
                    )));
                }
            }
        }
        drop(hydrated);

        Some((
            SyncEvent::Batch(Batch {
                items,
                page_boundary: if final_batch {
                    PageBoundary::Final
                } else {
                    PageBoundary::Page
                },
                server_latency: started.elapsed(),
                bytes_in: state.tally.take(),
                checkpoint: None,
            }),
            state,
        ))
    }))
}

struct HydrateState {
    client: Arc<GmailClient>,
    tally: crate::client::ByteTally,
    cache: ScopeCache,
    ids: AccountStream<ObjectId>,
    projection: Projection,
    finished: bool,
    emitted_done: bool,
    shutdown: CancellationToken,
}

/// Decides whether the walk must refuse to follow `next_page_token`,
/// returning the diagnostic detail for the terminating error.
///
/// Guard ORDER is deliberate and matches `changes::walk_refusal` and
/// `calendar::calendars_list`: the repeated-token check runs FIRST. Both
/// guards terminate the walk, so the engine sees the same outcome either
/// way, but a page that trips both deserves the sharper diagnosis. A
/// repeated token names a provider contract breach - the server is cycling
/// and no amount of further paging makes progress - where the budget only
/// reports "too many pages", which is also what an honestly huge mailbox
/// looks like.
fn inventory_walk_refusal(
    seen_page_tokens: &mut HashSet<String>,
    pages_walked: usize,
    next_page_token: Option<&str>,
) -> Option<PageRefusal> {
    let token = next_page_token?;
    if !seen_page_tokens.insert(token.to_string()) {
        return Some(PageRefusal::RepeatedToken(format!(
            "gmail users.messages.list repeated page token {token:?}"
        )));
    }
    (pages_walked >= MAX_INVENTORY_PAGES).then(|| {
        PageRefusal::BudgetExceeded(format!(
            "gmail users.messages.list exceeded {MAX_INVENTORY_PAGES} pages in one walk"
        ))
    })
}

async fn list_messages_page(
    client: &GmailClient,
    page_token: Option<&str>,
) -> crate::Result<ListMessagesResponse> {
    let path = inventory_list_path(page_token);
    client.get(&path).await
}

fn inventory_list_path(page_token: Option<&str>) -> String {
    format!(
        "/messages{}",
        crate::api::mail_list_query(None, Some(LIST_PAGE_SIZE), page_token)
    )
}

fn inventory_checkpoint(profile: &GmailProfile) -> crate::Result<Option<Checkpoint>> {
    let history_id = profile.history_id.parse::<u64>().map_err(|error| {
        // `profile` is a complete Google response. A present but unusable
        // historyId is Google's parse failure, not malformed caller input.
        crate::Error::provider_response(
            crate::error::ProviderFault::ParseFailed,
            format!("gmail profile carried invalid history id: {error}"),
        )
    })?;
    Ok(Some(Checkpoint::Change(cursor_for_history(
        history_id,
        &profile.email_address,
    ))))
}

async fn hydrate_one(
    client: &GmailClient,
    label_names: &flags::LabelNameIndex,
    id: ObjectId,
    projection: Projection,
) -> crate::Result<HydratedObject> {
    match projection {
        Projection::FlagsOnly => {
            let message = client.get_message(&id.0, "minimal").await?;
            Ok(HydratedObject {
                id,
                kind: HydratedObjectKind::FlagsOnly(flags::flag_set_indexed(
                    &message.label_ids,
                    label_names,
                )),
                blobs: Vec::new(),
            })
        }
        Projection::Metadata => {
            let message = client.get_message(&id.0, "metadata").await?;
            Ok(HydratedObject {
                id,
                kind: HydratedObjectKind::Metadata(inventory_entry_from_message_indexed(
                    &message,
                    label_names,
                )),
                blobs: Vec::new(),
            })
        }
        Projection::FullWithBlobs => {
            // Two calls are irreducible here, and they are issued
            // CONCURRENTLY rather than one after the other.
            //
            // Irreducible: `format=raw` is the only format that returns the
            // original MIME bytes, and it returns nothing else - no `payload`
            // part tree. `format=full` is the only format that returns the
            // part tree carrying `body.attachmentId`, which is the id
            // `open_blob` needs. That id is minted by Gmail and appears
            // nowhere in the message's own bytes, so it cannot be recovered
            // by parsing the raw MIME; and `format=metadata` returns headers
            // only, no parts, so it cannot supply it either.
            //
            // The second call is also far cheaper than it looks: `format=full`
            // inlines `body.data` only for parts WITHOUT an `attachmentId`,
            // so a message with a 20 MB attachment does not transfer those
            // 20 MB twice. The duplicated payload is the inline text bodies.
            //
            // What the pairing does cost is quota (5 units each) and, before
            // this was joined, a second full round trip of latency per
            // message on the hottest projection in the read path.
            let (raw, full) = futures::future::try_join(
                client.get_message(&id.0, "raw"),
                client.get_message(&id.0, "full"),
            )
            .await?;
            Ok(HydratedObject {
                id,
                kind: HydratedObjectKind::RawMime(raw_bytes(&raw)?),
                blobs: blobs::blob_handles_for_message(&full),
            })
        }
        Projection::Headers | Projection::Preview(_) | Projection::TextOnly | Projection::Full => {
            let message = client.get_message(&id.0, "raw").await?;
            Ok(HydratedObject {
                id,
                kind: HydratedObjectKind::RawMime(raw_bytes(&message)?),
                blobs: Vec::new(),
            })
        }
        _ => {
            let message = client.get_message(&id.0, "metadata").await?;
            Ok(HydratedObject {
                id,
                kind: HydratedObjectKind::Metadata(inventory_entry_from_message_indexed(
                    &message,
                    label_names,
                )),
                blobs: Vec::new(),
            })
        }
    }
}

pub(crate) fn raw_bytes(message: &GmailMessage) -> crate::Result<Bytes> {
    let raw = message.raw.as_deref().ok_or_else(|| {
        crate::Error::missing_field("raw", "gmail raw projection did not include raw bytes")
    })?;
    let bytes = decode_base64url_nopad(raw)?;
    Ok(Bytes::from(bytes))
}

#[cfg(test)]
pub(crate) fn inventory_entry_from_message(
    message: &GmailMessage,
    labels: &[GmailLabel],
) -> InventoryEntry {
    let label_names = flags::label_name_index(labels);
    inventory_entry_from_message_indexed(message, &label_names)
}

fn inventory_entry_from_message_indexed(
    message: &GmailMessage,
    label_names: &flags::LabelNameIndex,
) -> InventoryEntry {
    let size = non_negative_u64(message.size_estimate);
    let canonical = flags::canonical_flags_indexed(&message.label_ids, label_names);
    InventoryEntry {
        id: ObjectId(message.id.clone()),
        memberships: message
            .label_ids
            .iter()
            .map(|label| MembershipScope::Label(LabelId(label.clone())))
            .collect(),
        size,
        blob_id: None,
        fingerprint: Fingerprint {
            server_version: message
                .history_id
                .as_deref()
                .and_then(|id| id.parse::<u64>().ok())
                .map_or(ServerVersion::Unavailable, ServerVersion::HistoryAt),
            size,
            flags_hash: canonical.hash,
        },
        thread_id: Some(ThreadId(message.thread_id.clone())),
        message_id: header(message, "Message-ID"),
        references: header(message, "References").map_or_else(Vec::new, |value| {
            value
                .split_whitespace()
                .map(|item| item.trim_matches(|ch| ch == '<' || ch == '>').to_string())
                .collect()
        }),
        in_reply_to: header(message, "In-Reply-To"),
    }
}

fn header(message: &GmailMessage, name: &str) -> Option<String> {
    find_header_value_case_insensitive(
        headers(message),
        name,
        |h| h.name.as_str(),
        |h| h.value.as_str(),
    )
}

fn headers(message: &GmailMessage) -> &[GmailHeader] {
    message
        .payload
        .as_ref()
        .map_or(&[] as &[GmailHeader], |payload| payload.headers.as_slice())
}

fn non_negative_u64(value: Option<i64>) -> Option<u64> {
    value.and_then(|value| u64::try_from(value).ok())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use bifrost_net::test_support::{Canned, ScriptedDispatch, canned};
    use bifrost_net::{NetConfig, RetryPolicy, StaticTokenSource};
    use futures::StreamExt;
    use reqwest::StatusCode;

    use super::*;
    use serde_json::json;

    fn message(value: serde_json::Value) -> GmailMessage {
        serde_json::from_value(value).expect("message fixture deserializes")
    }

    fn with_headers(id: &str, headers: serde_json::Value) -> GmailMessage {
        message(json!({
            "id": id,
            "threadId": format!("thread-{id}"),
            "labelIds": ["INBOX"],
            "historyId": "12345",
            "sizeEstimate": 2048,
            "payload": { "mimeType": "text/plain", "headers": headers },
        }))
    }

    fn user_label() -> Vec<GmailLabel> {
        vec![GmailLabel {
            id: "Label_1".to_owned(),
            name: "Work".to_owned(),
            label_type: Some("user".to_owned()),
            color: None,
        }]
    }

    fn scripted_client(steps: Vec<Canned>) -> Arc<GmailClient> {
        let script = ScriptedDispatch::new(steps);
        let token_source = Arc::new(StaticTokenSource::new("token", None));
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            token_source,
            RetryPolicy::disabled(),
        );
        Arc::new(GmailClient::with_account_net("https://gmail.test", net))
    }

    fn ok_json(value: serde_json::Value) -> Canned {
        let body = serde_json::to_vec(&value).expect("fixture serializes");
        Canned::Response {
            status: StatusCode::OK,
            headers: reqwest::header::HeaderMap::new(),
            body: Bytes::from(body),
        }
    }

    #[test]
    fn non_negative_conversion_drops_negatives_and_absent_values() {
        assert_eq!(non_negative_u64(Some(0)), Some(0));
        assert_eq!(non_negative_u64(Some(2048)), Some(2048));
        assert_eq!(non_negative_u64(None), None);
        assert_eq!(
            non_negative_u64(Some(-1)),
            None,
            "a negative size must not wrap into a huge u64"
        );
        assert_eq!(non_negative_u64(Some(i64::MIN)), None);
    }

    #[test]
    fn inventory_entry_projects_ids_memberships_and_size() {
        let msg = with_headers("m1", json!([]));
        let entry = inventory_entry_from_message(&msg, &user_label());

        assert_eq!(entry.id, ObjectId("m1".to_owned()));
        assert_eq!(entry.thread_id, Some(ThreadId("thread-m1".to_owned())));
        assert_eq!(entry.size, Some(2048));
        assert_eq!(entry.fingerprint.size, Some(2048));
        assert_eq!(
            entry.memberships,
            vec![MembershipScope::Label(LabelId("INBOX".to_owned()))]
        );
        assert!(
            entry.blob_id.is_none(),
            "the inventory pass carries no blob handle"
        );
    }

    /// Gmail's per-message `historyId` becomes the fingerprint's server
    /// version; a missing or unparseable one degrades to `Unavailable`
    /// rather than to a bogus zero.
    #[test]
    fn server_version_comes_from_the_messages_history_id() {
        let msg = with_headers("m2", json!([]));
        assert_eq!(
            inventory_entry_from_message(&msg, &[])
                .fingerprint
                .server_version,
            ServerVersion::HistoryAt(12345)
        );

        let no_history = message(json!({ "id": "m3", "threadId": "t3" }));
        assert_eq!(
            inventory_entry_from_message(&no_history, &[])
                .fingerprint
                .server_version,
            ServerVersion::Unavailable
        );

        let junk_history =
            message(json!({ "id": "m4", "threadId": "t4", "historyId": "not-a-number" }));
        assert_eq!(
            inventory_entry_from_message(&junk_history, &[])
                .fingerprint
                .server_version,
            ServerVersion::Unavailable
        );
    }

    #[test]
    fn threading_headers_are_read_case_insensitively() {
        let msg = with_headers(
            "m5",
            json!([
                { "name": "message-id", "value": "<a@example.test>" },
                { "name": "IN-REPLY-TO", "value": "<parent@example.test>" },
            ]),
        );
        let entry = inventory_entry_from_message(&msg, &[]);
        assert_eq!(entry.message_id.as_deref(), Some("<a@example.test>"));
        assert_eq!(
            entry.in_reply_to.as_deref(),
            Some("<parent@example.test>"),
            "header lookup must not depend on Gmail's capitalisation"
        );
    }

    /// `References` is split on whitespace and the angle brackets are
    /// trimmed, so the entry carries bare message ids.
    #[test]
    fn references_are_split_and_unbracketed() {
        let msg = with_headers(
            "m6",
            json!([{
                "name": "References",
                "value": "<a@example.test> <b@example.test>\r\n\t<c@example.test>",
            }]),
        );
        let entry = inventory_entry_from_message(&msg, &[]);
        assert_eq!(
            entry.references,
            vec![
                "a@example.test".to_owned(),
                "b@example.test".to_owned(),
                "c@example.test".to_owned(),
            ]
        );
    }

    #[test]
    fn absent_threading_headers_yield_empty_projections() {
        let msg = with_headers("m7", json!([]));
        let entry = inventory_entry_from_message(&msg, &[]);
        assert!(entry.message_id.is_none());
        assert!(entry.in_reply_to.is_none());
        assert!(entry.references.is_empty());
    }

    /// The fingerprint's `flags_hash` is what the engine compares to
    /// decide an object changed. It must move when the label set moves
    /// and must not move for an identical set.
    #[test]
    fn flags_hash_is_stable_for_a_given_label_set() {
        let unread = message(json!({
            "id": "m8", "threadId": "t8", "labelIds": ["INBOX", "UNREAD"],
        }));
        let read = message(json!({
            "id": "m8", "threadId": "t8", "labelIds": ["INBOX"],
        }));
        let a = inventory_entry_from_message(&unread, &[])
            .fingerprint
            .flags_hash;
        let b = inventory_entry_from_message(&unread, &[])
            .fingerprint
            .flags_hash;
        let c = inventory_entry_from_message(&read, &[])
            .fingerprint
            .flags_hash;
        assert_eq!(a, b, "the same label set must hash identically");
        assert_ne!(a, c, "flipping UNREAD must move the hash");
    }

    /// `canonical_flags` renders a user label as
    /// `$gmail-label:<id>:<name>` and falls back to the id when the
    /// label list does not know the id. The stream entry points refresh
    /// the vocabulary before calling this helper so they cannot persist
    /// the fallback hash on a cold start.
    #[test]
    fn an_empty_label_list_changes_the_flags_hash_for_user_labels() {
        let msg = message(json!({
            "id": "m9", "threadId": "t9", "labelIds": ["Label_1"],
        }));
        let with_names = inventory_entry_from_message(&msg, &user_label());
        let without_names = inventory_entry_from_message(&msg, &[]);
        assert_ne!(
            with_names.fingerprint.flags_hash, without_names.fingerprint.flags_hash,
            "the same message hashes differently depending on whether the label \
             cache happened to be populated",
        );
    }

    // ---- raw_bytes ----------------------------------------------------

    #[test]
    fn raw_bytes_decodes_base64url_without_padding() {
        // "From: a@b\r\n\r\nhi" base64url, unpadded.
        let msg = message(json!({
            "id": "m10",
            "threadId": "t10",
            "raw": "RnJvbTogYUBiDQoNCmhp",
        }));
        let bytes = raw_bytes(&msg).expect("raw projection decodes");
        assert_eq!(bytes.as_ref(), b"From: a@b\r\n\r\nhi");
    }

    #[test]
    fn raw_bytes_rejects_a_message_without_raw_octets() {
        let msg = message(json!({ "id": "m11", "threadId": "t11" }));
        assert!(
            raw_bytes(&msg).is_err(),
            "a raw projection that came back without raw bytes is a protocol fault, \
             not an empty message",
        );
    }

    #[test]
    fn raw_bytes_rejects_undecodable_base64() {
        let msg = message(json!({ "id": "m12", "threadId": "t12", "raw": "!!!not base64!!!" }));
        assert!(raw_bytes(&msg).is_err());
    }

    // ---- paging constants ---------------------------------------------

    #[test]
    fn page_and_batch_sizes_stay_inside_the_gmail_limits() {
        assert_eq!(
            LIST_PAGE_SIZE, 500,
            "500 is the users.messages.list maximum"
        );
        assert_eq!(HYDRATE_BATCH_SIZE, 32);
    }

    #[test]
    fn inventory_page_guard_refuses_repetition_and_budget_exhaustion() {
        let mut seen = HashSet::new();
        assert!(inventory_walk_refusal(&mut seen, 1, Some("next")).is_none());
        let repeated =
            inventory_walk_refusal(&mut seen, 2, Some("next")).expect("repeated token must refuse");
        assert!(matches!(repeated, PageRefusal::RepeatedToken(_)));
        assert!(repeated.detail().contains("repeated page token"));
        let mut fresh = HashSet::new();
        let budget = inventory_walk_refusal(&mut fresh, MAX_INVENTORY_PAGES, Some("fresh"))
            .expect("budget must refuse");
        assert!(matches!(budget, PageRefusal::BudgetExceeded(_)));
        assert!(budget.detail().contains("exceeded"));
        assert!(inventory_walk_refusal(&mut fresh, MAX_INVENTORY_PAGES, None).is_none());
    }

    /// A page that trips BOTH guards is diagnosed as the repeated token,
    /// not as budget exhaustion: the repeated token is the sharper claim,
    /// and this lane must agree with `changes::walk_refusal`.
    #[test]
    fn a_page_tripping_both_guards_is_diagnosed_as_the_repeated_token() {
        let mut seen = HashSet::new();
        seen.insert("cycling".to_string());
        let refusal = inventory_walk_refusal(&mut seen, MAX_INVENTORY_PAGES, Some("cycling"))
            .expect("a page tripping both guards must refuse");
        assert!(matches!(refusal, PageRefusal::RepeatedToken(_)));
        let detail = refusal.detail();
        assert!(
            detail.contains("repeated page token"),
            "repeated-token detection must win over the budget: {detail}",
        );
        assert!(
            !detail.contains("exceeded"),
            "the budget must not claim a page the repeated-token guard owns: {detail}",
        );
    }

    // A reserved concurrency claim is not yet a recorded wire request:
    // admission, rate and token futures precede polling ScriptedDispatch.
    // Drive those stages with a bounded poll/yield loop before inspecting
    // counts. The operation must stay pending on a claim or canned response.
    async fn drive_pending<F: std::future::Future>(mut request: std::pin::Pin<&mut F>) {
        for _ in 0..64 {
            assert!(futures::FutureExt::now_or_never(request.as_mut()).is_none());
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test]
    async fn concurrent_hydration_walks_share_one_gmail_data_bound() {
        let mut steps = vec![ok_json(json!({ "labels": [] }))];
        steps.extend((0..HYDRATE_BATCH_SIZE + 1).map(|_| Canned::Pending));
        let script = ScriptedDispatch::new(steps);
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            Arc::new(StaticTokenSource::new("token", None)),
            RetryPolicy::disabled(),
        );
        // Separate clients model overlapping opens on the same Net/account,
        // rather than relying on clones sharing a client-local handle.
        let first = Arc::new(GmailClient::with_account_net(
            "https://gmail.test",
            net.clone(),
        ));
        let second = Arc::new(GmailClient::with_account_net("https://gmail.test", net));
        let walk = |client| {
            get_stream(
                client,
                Arc::new(super::super::scopes::ScopeCacheState::new(
                    super::super::scopes::ScopeSnapshot::empty(),
                )),
                Box::pin(stream::iter(
                    (0..HYDRATE_BATCH_SIZE).map(|index| ObjectId(format!("m{index}"))),
                )),
                Projection::Metadata,
            )
        };
        let mut first = walk(first);
        let mut second = walk(second);
        let mut first_next = first.next();
        let mut second_next = second.next();
        drive_pending(std::pin::Pin::new(&mut first_next)).await;
        assert_eq!(script.requests().len(), HYDRATE_BATCH_SIZE + 1);
        drive_pending(std::pin::Pin::new(&mut second_next)).await;
        assert_eq!(
            script.requests().len(),
            HYDRATE_BATCH_SIZE + 1,
            "the second walk's label refresh must share the saturated bound"
        );
        drop(first_next);
        drop(first);
        drive_pending(std::pin::Pin::new(&mut second_next)).await;
        assert_eq!(
            script.requests().len(),
            HYDRATE_BATCH_SIZE + 2,
            "dropping the first walk admits the second walk"
        );
        assert_eq!(crate::client::GMAIL_DATA_CONCURRENCY, HYDRATE_BATCH_SIZE);
    }

    #[tokio::test(start_paused = true)]
    async fn get_stream_starts_a_full_hydration_batch_concurrently_and_cancels_promptly() {
        let mut steps = vec![ok_json(json!({ "labels": [] }))];
        steps.extend((0..HYDRATE_BATCH_SIZE).map(|_| Canned::Pending));
        let script = ScriptedDispatch::new(steps);
        let token_source = Arc::new(StaticTokenSource::new("token", None));
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            token_source,
            RetryPolicy::disabled(),
        );
        let client = Arc::new(GmailClient::with_account_net("https://gmail.test", net));
        let ids = (0..HYDRATE_BATCH_SIZE).map(|index| ObjectId(format!("m{index}")));
        let shutdown = CancellationToken::new();
        let mut events = get_stream_cancellable(
            client,
            Arc::new(super::super::scopes::ScopeCacheState::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            Box::pin(stream::iter(ids)),
            Projection::Metadata,
            shutdown.clone(),
        );
        let started = tokio::time::Instant::now();
        let next = tokio::spawn(async move { events.next().await });
        for _ in 0..100 {
            if script.requests().len() == HYDRATE_BATCH_SIZE + 1 {
                break;
            }
            tokio::task::yield_now().await;
        }
        assert_eq!(
            script.requests().len(),
            HYDRATE_BATCH_SIZE + 1,
            "all hydration requests must overlap after the one label refresh"
        );
        shutdown.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), next)
                .await
                .expect("cancellation must beat the one-second deadline")
                .expect("hydration task joins")
                .is_none()
        );
        assert_eq!(started.elapsed(), std::time::Duration::ZERO);
        assert_eq!(script.requests().len(), HYDRATE_BATCH_SIZE + 1);
    }

    /// The label refresh a hydration batch needs happens before any
    /// `users.messages.get` is sent, so when it fails not one id of the
    /// drained batch was transmitted. The terminator must therefore scope
    /// the failure to the account-wide label vocabulary - naming an
    /// arbitrary first id pointed support exports at a message the failing
    /// request never mentioned and said nothing about the other 31 - and
    /// must not manufacture per-id lanes for ids nothing was attempted for.
    #[tokio::test]
    async fn a_failed_label_refresh_terminates_without_borrowing_one_id_of_the_batch() {
        let client = scripted_client(vec![canned(
            StatusCode::SERVICE_UNAVAILABLE,
            b"{\"error\":{\"code\":503,\"message\":\"backend error\"}}",
        )]);
        let ids = ["m1", "m2", "m3"].map(|id| ObjectId(id.to_owned()));
        let mut events = get_stream(
            client,
            Arc::new(super::super::scopes::ScopeCacheState::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            Box::pin(stream::iter(ids)),
            Projection::Metadata,
        );

        let first = events.next().await.expect("the stream must terminate");
        let SyncEvent::Terminated(error) = first else {
            panic!("a failed label refresh must terminate, got {first:?}");
        };
        assert_eq!(
            error.scope(),
            Some(&bifrost_types::ErrorScope::Account),
            "the label vocabulary is account-wide; the scope must not borrow a message id"
        );
        assert_eq!(error.operation(), Some(AccountOperation::HydrateMessage));
        assert!(
            events.next().await.is_none(),
            "Terminated ends the hydration stream"
        );
    }

    /// `repair_inventory` refreshes the label map before it can answer any
    /// request, and the same rule as the hydration terminator applies: the
    /// failing call is `labels.list`, so the error must name the
    /// account-wide label resource. It must NOT reuse the inventory WALK's
    /// context, whose `resource: Message` classifies a refusal as a missing
    /// or forbidden message and whose `Cursor(CursorScope::Account)` scope
    /// is the coordinate bifrost-sync routes cursor directives by - a
    /// transient `labels.list` fault has nothing to say about the cursor.
    #[tokio::test]
    async fn a_failed_repair_label_refresh_names_the_label_resource_not_the_cursor() {
        // A 404 pins the RESOURCE half as well as the scope: the walk's
        // `Message` resource would classify this as `NotFound(Message)`.
        let client = scripted_client(vec![canned(
            StatusCode::NOT_FOUND,
            b"{\"error\":{\"code\":404,\"message\":\"not found\"}}",
        )]);
        let request = bifrost_types::InventoryRepairRequest {
            attempt: bifrost_types::RepairAttemptId(1),
            key: bifrost_types::ObligationKey(b"k".to_vec()),
            domain: bifrost_types::CoverageDomain::full(CursorScope::Account),
            target: bifrost_types::InventoryRepairTarget::Object {
                id: ObjectId("m1".to_owned()),
                repair: b"tok".to_vec(),
            },
        };
        let mut events = repair_inventory(
            client,
            Arc::new(super::super::scopes::ScopeCacheState::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            Box::pin(stream::iter([request])),
        );

        let first = events.next().await.expect("the stream must terminate");
        let bifrost_types::InventoryRepairEvent::Terminated(error) = first else {
            panic!("a failed label refresh must terminate, got {first:?}");
        };
        assert_eq!(
            error.scope(),
            Some(&bifrost_types::ErrorScope::Account),
            "a labels.list fault must not carry the walk's cursor scope"
        );
        assert_eq!(
            error.kind(),
            &AccountErrorKind::NotFound(ResourceKind::Mailbox),
            "a labels.list 404 is a missing label, never a missing message"
        );
        assert!(
            events.next().await.is_none(),
            "Terminated ends the repair stream"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn inventory_cancellation_stops_an_inflight_request_without_an_extra_call() {
        let script = ScriptedDispatch::new(vec![Canned::Pending, Canned::Pending]);
        let token_source = Arc::new(StaticTokenSource::new("token", None));
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            token_source,
            RetryPolicy::disabled(),
        );
        let client = Arc::new(GmailClient::with_account_net("https://gmail.test", net));
        let shutdown = CancellationToken::new();
        let mut events = inventory_stream_cancellable(
            client,
            Arc::new(super::super::scopes::ScopeCacheState::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            CursorScope::Account,
            shutdown.clone(),
        );
        let started = tokio::time::Instant::now();
        let next = tokio::spawn(async move { events.next().await });
        while script.requests().is_empty() {
            tokio::task::yield_now().await;
        }
        shutdown.cancel();
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), next)
                .await
                .expect("cancellation must beat the one-second deadline")
                .expect("inventory task joins")
                .is_none()
        );
        assert_eq!(started.elapsed(), std::time::Duration::ZERO);
        assert_eq!(script.requests().len(), 1);
    }

    #[test]
    fn inventory_lists_spam_and_trash_and_encodes_its_page_token() {
        assert_eq!(
            inventory_list_path(None),
            "/messages?includeSpamTrash=true&maxResults=500"
        );
        assert_eq!(
            inventory_list_path(Some("next+page")),
            "/messages?includeSpamTrash=true&maxResults=500&pageToken=next%2Bpage"
        );
    }

    #[test]
    fn inventory_checkpoint_uses_the_profile_sampled_before_paging() {
        let checkpoint = inventory_checkpoint(&GmailProfile {
            email_address: "person@example.com".to_string(),
            history_id: "100".to_string(),
        })
        .expect("valid profile")
        .expect("change checkpoint");
        let Checkpoint::Change(cursor) = checkpoint else {
            panic!("expected a Gmail change checkpoint");
        };
        let state =
            super::super::cursor::decode_gmail_state(&cursor.server_state).expect("decode state");
        assert_eq!(state.history_id, 100);
        assert_eq!(state.profile_email, "person@example.com");
    }

    #[tokio::test]
    async fn inventory_profile_with_unparseable_history_id_is_a_provider_parse_failure() {
        let client = scripted_client(vec![ok_json(json!({
            "emailAddress": "person@example.com",
            "historyId": "not-a-number"
        }))]);
        let events = inventory_stream(
            client,
            Arc::new(super::super::scopes::ScopeCacheState::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            CursorScope::Account,
        )
        .collect::<Vec<_>>()
        .await;

        let bifrost_types::InventoryEvent::Terminated(error) = &events[0] else {
            panic!("an invalid provider history id must terminate inventory: {events:?}");
        };
        assert_eq!(
            error.kind(),
            &bifrost_types::AccountErrorKind::Protocol(
                bifrost_types::ProtocolErrorKind::ParseFailed
            )
        );
        assert!(error.chain().iter().any(|cause| matches!(
            cause,
            bifrost_types::Cause::Attempt(attempt)
                if attempt.transmission_state == bifrost_types::TransmissionState::Acknowledged
        )));
    }

    /// An unreadable object costs that object, not the whole partition.
    ///
    /// This is the defect the coverage model exists for. A classified
    /// hydration failure that is NOT the list/get deletion race used to yield
    /// `Terminated`, discarding every page the walk had already emitted and
    /// leaving no cursor - so the next attempt re-walked from the start, hit
    /// the same object, and failed the same way, forever.
    ///
    /// The walk now keeps going and records the object as an obligation, and
    /// the checkpoint DECLARES that gap rather than certifying coverage it
    /// does not have. Both halves are asserted: the surviving message is still
    /// delivered with a checkpoint, and the completion reports `Degraded`
    /// naming the object. A change that kept walking but claimed `Complete`
    /// would pass the first half while silently making the object permanently
    /// invisible - the cursor advances past it and the changes stream only
    /// reports what happens next.
    #[tokio::test]
    async fn an_unreadable_object_becomes_an_obligation_instead_of_killing_the_walk() {
        let client = scripted_client(vec![
            ok_json(json!({ "emailAddress": "person@example.com", "historyId": "100" })),
            ok_json(json!({ "labels": [] })),
            ok_json(json!({ "messages": [{ "id": "broken" }, { "id": "live" }] })),
            // Not a 404: a permission failure is not evidence of absence, so
            // nothing is discharged and an obligation is owed.
            canned(
                StatusCode::FORBIDDEN,
                br#"{"error":{"code":403,"message":"Forbidden","status":"PERMISSION_DENIED"}}"#,
            ),
            ok_json(json!({ "id": "live", "threadId": "thread-live" })),
        ]);

        let events = inventory_stream(
            client,
            Arc::new(super::super::scopes::ScopeCacheState::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            CursorScope::Account,
        )
        .collect::<Vec<_>>()
        .await;

        assert!(
            !events
                .iter()
                .any(|event| matches!(event, bifrost_types::InventoryEvent::Terminated(_))),
            "one unreadable object must not discard the walk: {events:?}"
        );

        let bifrost_types::InventoryEvent::Done(completion) = events.last().expect("events") else {
            panic!("the walk must finish, not stall: {events:?}");
        };
        assert!(
            completion.checkpoint.accepted(false).is_some(),
            "the scope must converge rather than re-walking forever"
        );
        assert!(
            !completion.coverage.is_complete(),
            "a checkpoint over an unrepresented object must declare the gap"
        );
        let obligations = completion.coverage.obligations();
        assert_eq!(obligations.len(), 1);
        assert!(
            matches!(
                &obligations[0],
                bifrost_types::InventoryObligation::Object { id, .. }
                    if id.0 == "broken"
            ),
            "the obligation must name the object so a repair can address it: {obligations:?}"
        );

        // The surviving message is still delivered, and its batch declares the
        // gap too - a page checkpoint certifies the results before it.
        let batch = events
            .iter()
            .find_map(|event| match event {
                bifrost_types::InventoryEvent::Batch(batch) => Some(batch),
                _ => None,
            })
            .expect("the surviving message must still be delivered");
        assert_eq!(batch.items.len(), 1);
        assert_eq!(batch.items[0].id, ObjectId("live".to_string()));
        assert!(!batch.coverage.is_complete());
    }

    /// The guard is only worth anything wired into the walk. A provider
    /// that hands back the same `nextPageToken` twice must end the
    /// stream with `Terminated` alone: no `Done`, no final boundary and
    /// no checkpoint, because a truncated enumeration published as
    /// coverage advances the cursor past objects it never listed.
    #[tokio::test]
    async fn a_repeated_inventory_page_token_terminates_without_claiming_coverage() {
        let client = scripted_client(vec![
            ok_json(json!({ "emailAddress": "person@example.com", "historyId": "100" })),
            ok_json(json!({ "labels": [] })),
            ok_json(json!({ "messages": [{ "id": "a" }], "nextPageToken": "loop" })),
            ok_json(json!({ "id": "a", "threadId": "thread-a" })),
            ok_json(json!({ "messages": [{ "id": "b" }], "nextPageToken": "loop" })),
        ]);

        let events = inventory_stream(
            client,
            Arc::new(super::super::scopes::ScopeCacheState::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            CursorScope::Account,
        )
        .collect::<Vec<_>>()
        .await;

        let last = events.last().expect("events");
        assert!(
            matches!(last, bifrost_types::InventoryEvent::Terminated(_)),
            "a looping walk must terminate: {events:?}"
        );
        assert!(
            !events
                .iter()
                .any(|event| matches!(event, bifrost_types::InventoryEvent::Done(_))),
            "a refused walk must never report completion: {events:?}"
        );
        assert!(
            events.iter().all(|event| match event {
                bifrost_types::InventoryEvent::Batch(batch) => {
                    batch.checkpoint.offered().is_none()
                        && !matches!(batch.page_boundary, PageBoundary::Final)
                }
                _ => true,
            }),
            "no batch of a refused walk may checkpoint or close the page run: {events:?}"
        );
    }

    #[tokio::test]
    async fn inventory_absorbs_a_list_get_not_found_race_and_finishes() {
        let client = scripted_client(vec![
            ok_json(json!({ "emailAddress": "person@example.com", "historyId": "100" })),
            ok_json(json!({ "labels": [] })),
            ok_json(json!({ "messages": [{ "id": "gone" }, { "id": "live" }] })),
            canned(
                StatusCode::NOT_FOUND,
                br#"{"error":{"code":404,"message":"Not Found","status":"NOT_FOUND"}}"#,
            ),
            ok_json(json!({ "id": "live", "threadId": "thread-live" })),
        ]);

        let events = inventory_stream(
            client,
            Arc::new(super::super::scopes::ScopeCacheState::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            CursorScope::Account,
        )
        .collect::<Vec<_>>()
        .await;

        assert_eq!(events.len(), 2);
        let bifrost_types::InventoryEvent::Batch(batch) = &events[0] else {
            panic!("inventory must emit its surviving final batch");
        };
        assert!(matches!(batch.page_boundary, PageBoundary::Final));
        assert_eq!(batch.items.len(), 1);
        assert_eq!(batch.items[0].id, ObjectId("live".to_string()));
        assert!(matches!(
            &events[1],
            bifrost_types::InventoryEvent::Done(completion)
                if completion.checkpoint.accepted(false).is_some()
        ));
    }

    #[tokio::test]
    async fn empty_inventory_emits_a_checkpointed_final_batch() {
        let client = scripted_client(vec![
            ok_json(json!({ "emailAddress": "person@example.com", "historyId": "100" })),
            ok_json(json!({ "labels": [] })),
            ok_json(json!({ "messages": [] })),
        ]);

        let events = inventory_stream(
            client,
            Arc::new(super::super::scopes::ScopeCacheState::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            CursorScope::Account,
        )
        .collect::<Vec<_>>()
        .await;

        assert_eq!(events.len(), 2);
        let bifrost_types::InventoryEvent::Batch(batch) = &events[0] else {
            panic!("empty inventory must still expose its final boundary");
        };
        assert!(batch.items.is_empty());
        assert!(matches!(batch.page_boundary, PageBoundary::Final));
        assert!(batch.checkpoint.accepted(false).is_some());
        assert!(matches!(
            &events[1],
            bifrost_types::InventoryEvent::Done(completion)
                if completion.checkpoint.accepted(false).is_some()
        ));
    }

    #[tokio::test]
    async fn get_stream_marks_its_last_nonempty_batch_final() {
        let client = scripted_client(vec![
            ok_json(json!({ "labels": [] })),
            ok_json(json!({ "id": "m1", "threadId": "t1" })),
            ok_json(json!({ "id": "m2", "threadId": "t2" })),
        ]);
        let ids = Box::pin(stream::iter([
            ObjectId("m1".to_string()),
            ObjectId("m2".to_string()),
        ]));

        let events = get_stream(
            client,
            Arc::new(super::super::scopes::ScopeCacheState::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            ids,
            Projection::Metadata,
        )
        .collect::<Vec<_>>()
        .await;

        assert_eq!(events.len(), 2);
        let SyncEvent::Batch(batch) = &events[0] else {
            panic!("hydration must emit a batch");
        };
        assert!(matches!(batch.page_boundary, PageBoundary::Final));
        assert_eq!(batch.items.len(), 2);
        assert!(matches!(events[1], SyncEvent::Done(None)));
    }

    // A backpressured producer hands over a full batch and then goes quiet
    // while it waits for hydration output. Hydration must emit that batch
    // without polling the id stream again; a lookahead poll to decide the
    // page boundary parks here forever. Time is paused, so the timeout fires
    // only once every other task is idle - that is the deadlock, not a race
    // against a wall clock.
    #[tokio::test(start_paused = true)]
    async fn hydration_emits_a_full_batch_without_waiting_for_another_id() {
        let mut script = vec![ok_json(json!({ "labels": [] }))];
        let mut ids = Vec::new();
        for index in 0..HYDRATE_BATCH_SIZE {
            let id = format!("m{index}");
            script.push(ok_json(json!({ "id": id, "threadId": "t" })));
            ids.push(ObjectId(id));
        }

        // Yields exactly one full batch, then stays open and pending.
        let id_stream = Box::pin(stream::iter(ids).chain(stream::once(futures::future::pending())));

        let mut events = get_stream(
            scripted_client(script),
            Arc::new(super::super::scopes::ScopeCacheState::new(
                super::super::scopes::ScopeSnapshot::empty(),
            )),
            id_stream,
            Projection::Metadata,
        );

        let first = tokio::time::timeout(std::time::Duration::from_secs(30), events.next())
            .await
            .expect("hydration must not block on an id beyond the batch it already holds");

        let Some(SyncEvent::Batch(batch)) = first else {
            panic!("hydration must emit the full batch it already holds");
        };
        assert_eq!(batch.items.len(), HYDRATE_BATCH_SIZE);
        // The producer is still open, so this batch cannot claim to be final.
        assert!(matches!(batch.page_boundary, PageBoundary::Page));
    }

    #[tokio::test]
    async fn full_with_blobs_issues_its_raw_and_full_fetches_concurrently() {
        // `FullWithBlobs` needs both `format=raw` (the only source of MIME
        // bytes) and `format=full` (the only source of `body.attachmentId`).
        // Neither depends on the other's answer, so serializing them costs a
        // whole round trip per message on the hottest read projection.
        //
        // Both canned answers carry BOTH shapes, so the assertion is about
        // fan-out and not about which leg happens to be answered first.
        let both_shapes = json!({
            "id": "m-blob",
            "threadId": "t-blob",
            "labelIds": ["INBOX"],
            "historyId": "9",
            "raw": "aGk",
            "payload": {
                "mimeType": "multipart/mixed",
                "parts": [{
                    "mimeType": "application/pdf",
                    "filename": "report.pdf",
                    "body": { "attachmentId": "att-1", "size": 2048 },
                }],
            },
        });

        let script =
            ScriptedDispatch::yielding(vec![ok_json(both_shapes.clone()), ok_json(both_shapes)]);
        let token_source = Arc::new(StaticTokenSource::new("token", None));
        let net = bifrost_net::test_support::scripted_account(
            &script,
            NetConfig::default(),
            Vec::new(),
            token_source,
            RetryPolicy::disabled(),
        );
        let client = GmailClient::with_account_net("https://gmail.test", net);

        let names = flags::label_name_index(&[]);
        let hydrated = hydrate_one(
            &client,
            &names,
            ObjectId("m-blob".to_owned()),
            Projection::FullWithBlobs,
        )
        .await
        .expect("both projections answered");

        assert!(matches!(hydrated.kind, HydratedObjectKind::RawMime(_)));
        assert_eq!(
            hydrated.blobs.len(),
            1,
            "the `full` leg still supplies the attachment blob handle"
        );
        assert_eq!(
            script.requests().len(),
            2,
            "exactly two message fetches, no more"
        );
        assert_eq!(
            script.peak_in_flight(),
            2,
            "the raw and full fetches must be in flight together, not chained"
        );
    }
}
