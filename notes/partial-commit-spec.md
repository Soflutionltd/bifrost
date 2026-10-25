# Partial commitment: implementation spec (GOOG-027)

Status: design sparred to consensus with a cold codex session (read-only
profile) on 2026-10-25, six exchanges, the last a consistency read of this document; the positions conceded on each side are
recorded in section 11. Cold-reviewed once (two independent reports,
consolidated and folded 2026-10-25; section 12 records what was folded and
what was rejected). Implementation not landed. Adjudicated 2026-10-25:
uncertain `Blocking` legs gate instead of forcing `Never` (3.4, section 12
item C-4).

## 0. Standing references (read them, do not just note that they exist)

Implementers and reviewers MUST read these before touching code. They are the
contract this spec is written against and the ground it is judged on.

- `reference/technical-implementation-spec.md` - the contract this document is
  written against.
- `reference/error-model.md` - the cross-cutting `AccountError` contract. Read
  all of it; this spec changes what `recovery()` means when a write partially
  committed, adds a builder input, two `ReconcileAdvice` vocabulary items, three
  `ErrorScope` variants and a telemetry field.
- `notes/bugs-deferred.md`, entry "GOOG-027 - Shared partial-commit
  representation and protocol adoption" - the originating item and its ruled
  scope and acceptance requirements. Every requirement there is mapped to a
  brick in section 10.
- `reference/types.md` - the shared type crate the representation lives in.
- `reference/sync.md` - "Mutation pipeline", "Read-back guard", "Ledger
  envelope" and "Caller-version mutation campaigns": the in-workspace consumer.
- `reference/imap.md`, `reference/jmap.md`, `reference/google.md`,
  `reference/graph.md`, `reference/caldav.md`, `reference/carddav.md` - the
  producer crates. `reference/smtp.md` - audited, no applicable producer
  (section 5.8).
- `reference/net.md` - "Replay safety": request replay policy stays exactly as
  it is; this spec sits above it.

## 1. The item, and whether it should be built

GOOG-027 asks for a typed shared representation of partial commitment: what
earlier steps of a write are known to have committed, whether the failed step
itself may have applied, what compensation ran and how it ended, and the
failed step's own recovery, preserved whole. Every multi-step write producer
adopts it; consumers must read it; recovery must say what it applies to.

The ruling was checked against the contract and the code before any design
work. Nothing gives reason not to build it:

- The defect is real and present today. A Google move followed by a refused
  PATCH returns the PATCH's `Retry(SameRequest)` for an `event_update` whose
  replay repeats the move (`crates/google/src/account/calendar.rs`,
  `event_move_patch_error`). IMAP's MOVE fallback lets an acknowledged
  retryable STORE refusal after COPY derive `Retry(SameRequest)`, and the bulk
  pipeline's `classify_item_outcome` puts that id in the retry queue, which
  re-runs COPY (`crates/imap/src/connection/uid_ops.rs`,
  `after_acknowledged_side_effect`; `crates/sync/src/engine/bulk.rs`). DAV and
  JMAP collapse later failures into `Protocol(PartialResponse)` and lose the
  leaf's retry hint, throttle scope and auth/permission recovery
  (`partial_sequence_error`, `partial_move_error`,
  `incomplete_multi_step_write`, `partial_email_set_error`).
- `AccountError` derives one recovery from one classification prefix, and
  `into_builder` deliberately freezes that prefix (`builder.rs`), so no
  existing input can say "this request was refused, and an earlier request
  committed". The representation has to be new.
- The cold partner independently concluded the item should be built
  (section 11).

## 2. Survey

### 2.1 The error type today

- `crates/types/src/error/account_error.rs`: `AccountError` is an
  `Arc<AccountErrorInner>`; `recovery` is derived once in `try_build` and is
  the single dispatch surface. `TelemetryView::from_account_error` projects one
  recovery and scans the chain for the first `Attempt`.
- `crates/types/src/error/builder.rs`: `try_build` validates, then calls
  `recovery::derive(kind, scope, operation, classification_chain,
  throttle_scope, idempotency_override)` and `recovery::suggest`.
  `classification_cause_count` freezes the classification prefix across
  rebuilds.
- `crates/types/src/error/recovery.rs`: `derive`, `derive_protocol`
  (`PartialResponse` -> `Retry(SameRequest)` when idempotent, else
  `Reconcile(PartialCompletionSignal, [CheckTarget] + DedupeByClientId when the
  table is non-idempotent)`), `transient_retry_or_reconcile`,
  `derive_internal`, `retry_later`, `Fatal`.
- `crates/types/src/error/scope.rs`: `ErrorScope` has no submission, cloud file
  or upload session identity.
- `crates/sync/src/cursor/ledger_envelope.rs`: `encode_error_scope` panics on an
  `ErrorScope` variant it does not know. Ledger rows record sync
  coverage/repair failures, never write failures.

### 2.2 Consumers

- `crates/sync/src/engine/bulk.rs` dispatches each bulk item and stream
  termination on `error.recovery()`: `classify_item_outcome` (Retry
  SameRequest/AfterAuthRefresh -> retry queue; AfterStateRefresh and Reconcile
  -> read-back; Engine -> BlockedByEngine; terminal -> FailedTerminal),
  `uncertain_origin` and `reconcile_origin` (Reassertable iff every action is
  `CheckTarget`), `reassertion_declared` (moves never reassert),
  `record_error_throttle` (Retry and Reconcile only), and the `Terminated`
  arm's `record_throttle` / `record_reconcile_throttle`.
- `crates/sync/src/mutation/mod.rs`: `MutationCounters`.
- `crates/sync/src/recovery.rs`: `plan_recovery`, `record_throttle_parts`
  (resolves `ThrottleScope::Mailbox` against the error's own scope and records
  stamped budgets).
- `crates/sync/src/engine/passthrough.rs` forwards every direct PIM write 1:1;
  their consumers are downstream and read `recovery()`.

### 2.3 Producer inventory

Every write that issues more than one effect-bearing request, or one request
whose effect can be uncertain at a send boundary, across the workspace.
"Applicable" means it has a failure path that returns `Err` or a failed /
uncertain lane after an earlier effect.

| Crate | Path | Shape | Today |
|---|---|---|---|
| caldav | `relocate_event` MOVE then PUT | sequential | `partial_move_error` reclassifies to PartialResponse |
| caldav | `relocate_event` PUT If-None-Match then DELETE | sequential | same |
| caldav | `event_rsvp` outbox POST then PUT (incl. local encode failures between) | sequential | `rsvp_local_write_error` -> PartialResponse |
| carddav | `relocate_contact` MOVE then PUT; PUT then DELETE | sequential | `partial_move_error` |
| google | `calendar::update` move then PATCH | sequential | `event_move_patch_error` decorates text only |
| google | `contacts::update` updateContact then photo | sequential | `contact_fields_photo_error` decorates text only |
| google | `cloud::upload_and_link` session create, chunks, cancel, permission POST, `webViewLink` GET | sequential + compensation | disposition swallowed; abandoned session as text; a committed permission followed by a failed GET is reported as the permission's failure |
| graph | `pim/send.rs` `send_message` draft create (incl. a 2xx without an id), deferred stamp, send | sequential | no record; orphan draft unreported |
| graph | `pim/send.rs` `send_raw_message` MIME import then send | sequential | same |
| graph | `pim/messages.rs` `submit_write_batch_resource` (patch, move, destroy, thread ops in `pim/threads.rs`) | independent siblings | returns first failure in response order, drops successes, the new ids of successful moves, and the missing-id sweep |
| graph | `pim/threads.rs` `delete_thread` trash branch: one `permanentDelete` batch per `batching_policy.max_items` chunk | sequential over sibling batches | earlier committed chunks dropped |
| graph | `cloud::upload_and_link` session create, `admit_upload_url`, chunks, cancel, createLink | sequential + compensation | cancel outcome only logged |
| jmap | `send_message`, `send_raw_message` (Email create/import + submission in one request) | one request, cleanup compensation | `submission_create_error`, `incomplete_multi_step_write`, cleanup result discarded |
| jmap | `draft_send` | one request | generic mapping for missing/malformed submission result |
| jmap | `draft_create` acknowledged create without usable id | one request | `incomplete_multi_step_write` |
| jmap | `reschedule_send` cancel then create | sequential | `incomplete_multi_step_write` |
| jmap | `set_keyword`, `patch_mailbox_membership_of`, `destroy_thread_emails_if_unchanged` chunked `Email/set` | independent siblings | `partial_email_set_error`; stops at first failing id |
| jmap | `move_thread`, trash branch of `delete_thread` (two legs) | sequential over siblings | inner helper only |
| imap | `uid_move_messages` COPY, STORE \Deleted, UID EXPUNGE | sequential | `after_acknowledged_side_effect` evidence floor |
| imap | `mutate.rs` `run_destroy_mutation_groups` STORE then EXPUNGE | sequential | `expunge_failed_after_delete_mark` text |
| imap | `mutate.rs` `run_patch_mutation_group` two STOREs | sequential | second failure keeps leaf, no record |
| imap | `pim.rs` `copy_messages`, `delete_messages`, `set_flag` per folder, reached through the public `add_to_container`, `remove_from_container`, `set_keyword`, `set_is_read`, `set_importance`, `draft_discard` | independent siblings | first folder failure returned, earlier folders dropped |
| imap | `pim.rs` `move_thread` (calls the public `add_to_container` then `remove_from_container`), `delete_thread` (calls the public `move_thread` or `remove_from_container`) | sequential over siblings | inner errors |
| imap | `pim.rs` `container_create`, `container_rename`, `container_move`, `container_delete` then `refresh_folders` | write then read | refresh failure returned as the operation's error |

Audited, no applicable producer (each gets the reason recorded in its
reference doc, section 8):

- `bifrost-smtp`: one SMTP transaction; post-DATA per-recipient outcomes
  already ride the three lanes (`batch.rs`, `partial_completion_error`).
- `bifrost-google` `mutation.rs` chunked `batchModify`: per-item lanes
  already, including the uncertain lane for later chunk failures.
- Graph and Google bulk mutation streams and reaction reads: per-item lanes.
- JMAP `filters.rs` create/update and `send_raw_message` blob upload: an
  uploaded blob is unreferenced temporary storage the server collects; replay
  creates another, nothing visible duplicates.
- JMAP `calendar_ops.rs` and `contacts.rs` writes: one `set` call each.
- DAV create/delete, Google/Graph/JMAP single-request event and contact writes.
- Single-request creates whose acknowledged response lacks a usable id (IMAP
  `draft_create` without APPENDUID, Google `contacts::create` without
  `resourceName`, Graph `draft_create` via `create_draft_message`, Google and
  Graph filter creates, Google calendar event create): one effect-bearing
  request, so not a multi-step write and outside the ruling. They do carry a
  real defect shape (a committed but unidentified create classified terminal
  invites a duplicating manual retry), filed in section 9. JMAP `draft_create`
  is the one exception adopted here (5.4), and only because this spec deletes
  the `incomplete_multi_step_write` helper that already protects it and the
  ruling names its control test; Graph `create_draft_message` is adopted only
  where `send_message` calls it as its first leg (5.3).
- IMAP `draft_send` post-send `delete_messages` discard: it calls the private
  helper and logs the failure; there is no failure path (section 9).
- IMAP `send_message` / `send_raw_message` / `draft_send` Sent APPEND and
  draft discard after a committed submission: these return `Ok`; there is no
  failure path. The silent loss is a separate finding (section 9).

## 3. Target representation (`bifrost-types`)

New module `crates/types/src/error/partial.rs`, tests in
`crates/types/src/error/partial_tests.rs` (`#[cfg(test)] #[path = ...] mod
tests;`), re-exported from `error/mod.rs` and the crate root. All public enums
`#[non_exhaustive]`; all structs `#[non_exhaustive]` with constructors where
producers build them.

### 3.1 Types

```rust
/// Assigned sequentially by PartialCommitBuilder; primary is StepId(0).
/// Index-sized: a JMAP chunked thread operation records one step per
/// confirmed id across any number of chunks, so there is no width at which
/// allocation could overflow short of memory. Serialized as u64.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
pub struct StepId(usize);

#[non_exhaustive] #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Serialize)]
pub enum StepEffect { Created, Updated, Moved, Copied, MarkedDeleted, Deleted, Submitted, Cancelled }

/// Relative to the PUBLIC operation that attaches the record. The line
/// between the two is the operation's INTENDED effect, not visibility.
#[non_exhaustive] #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum StepReplay {
    /// Replaying the public operation re-applies this effect as a no-op
    /// success, or leaves at most a by-product that is not the operation's
    /// intended effect: an orphan draft of a send, an unlinked uploaded file,
    /// an expiring upload session. Such a by-product may be user-visible (an
    /// orphan draft sits in Drafts); it is accepted clutter, bounded by the
    /// retry budget, not duplication.
    Repeatable,
    /// Replaying would duplicate the operation's intended effect (a second
    /// sent message, a second draft from `draft_create`, a second copy at a
    /// destination), address an identity this effect changed, or be refused
    /// by a precondition this effect made false.
    Blocking,
}

#[non_exhaustive] #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum Application { NotApplied, Uncertain, NotApplicable }

impl Application {
    /// The application an effect-bearing leg's own leaf supports, read from
    /// its classification prefix only (never decoration):
    /// explicit `InFlight` attempt -> Uncertain; else leaf `recovery()` is
    /// `Reconcile` -> Uncertain; else NotApplied. Producers use this for
    /// every effect-bearing leg (3.2), so the 3.3 rules cannot be violated
    /// by any leaf a mapper can build.
    pub fn from_leaf(leaf: &AccountError) -> Self;
}

#[non_exhaustive] #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum TargetRole { Subject, Source, Destination }

#[non_exhaustive] #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct TargetRef { pub role: TargetRole, pub scope: ErrorScope }
impl TargetRef { pub fn new(role: TargetRole, scope: ErrorScope) -> Self; }

/// `complete == false` with empty `refs` means "identity unknown", never
/// "no target". `complete == false` with refs means "at least these".
#[non_exhaustive] #[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct StepTargets { pub refs: Vec<TargetRef>, pub complete: bool }
impl StepTargets {
    pub fn complete(refs: Vec<TargetRef>) -> Self;  // complete: true
    pub fn at_least(refs: Vec<TargetRef>) -> Self;  // complete: false
    pub fn unknown() -> Self;                        // refs empty, complete: false
}

#[non_exhaustive] #[derive(Clone, Debug, Eq, PartialEq)]
pub struct PrimaryLeg {               // the AccountError's own leaf IS this leg
    pub effect: Option<StepEffect>,   // None for an effectless leg (a read, a local refusal)
    pub targets: StepTargets,
    pub replay: StepReplay,
    pub application: Application,
}
impl PrimaryLeg {
    pub fn new(effect: StepEffect, targets: StepTargets, replay: StepReplay, application: Application) -> Self;
    /// effect None, application NotApplicable, replay Repeatable (unread for an effectless leg).
    pub fn effectless(targets: StepTargets) -> Self;
}

#[non_exhaustive] #[derive(Clone, Debug)]
pub struct Step { id: StepId, effect: Option<StepEffect>, targets: StepTargets, replay: StepReplay, outcome: StepOutcome }
#[non_exhaustive] #[derive(Clone, Debug)]
pub enum StepOutcome { Committed, Failed { application: Application, error: AccountError } }

#[non_exhaustive] #[derive(Clone, Debug)]
pub struct Compensation { id: StepId, reverses: Vec<StepId>, effect: StepEffect, targets: StepTargets, outcome: CompensationOutcome }
#[non_exhaustive] #[derive(Clone, Debug)]
pub enum CompensationOutcome { Succeeded, Failed(AccountError), Unconfirmed(AccountError) }
impl CompensationOutcome {
    /// Explicit `InFlight` or absent attempt evidence -> Unconfirmed; else
    /// Failed. A producer may declare Unconfirmed over Failed when it has a
    /// stated reason (JMAP's missing `destroyed(id)` result), never the
    /// reverse.
    pub fn from_error(error: AccountError) -> Self;
}

#[non_exhaustive] #[derive(Clone, Debug)]
pub struct PartialCommit {            // stored as Option<Arc<PartialCommit>> in AccountErrorInner
    primary: PrimaryLeg,
    steps: Vec<Step>,
    compensations: Vec<Compensation>,
    plan: OperationPlan,              // derived in try_build, never producer-supplied
}

#[non_exhaustive] #[derive(Clone, Debug)]
pub struct OperationPlan {
    pub gating: Vec<StepId>,
    pub residual: Vec<StepId>,
    pub replay: ReplaySafety,
    pub prerequisites: Prerequisites, // { auth_refresh: bool, state_refresh: bool }
    pub directives: Vec<PlannedDirective>, // { directive: EngineDirective, origins: Vec<StepId> }
    pub barriers: Vec<(StepId, RecoveryClass)>,
    pub leg_actions: Vec<ReconcileAction>,
    pub may_duplicate: bool,          // 3.4; drives DedupeByClientId in 3.5
}

#[non_exhaustive] #[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum ReplaySafety { Safe, AfterObservation, Never }
```

`Prerequisites` and `PlannedDirective` are `#[non_exhaustive]` with
`Clone, Debug` (they ride inside `AccountErrorInner`, which derives both).
Accessors on `PartialCommit`: `primary()`, `steps()`, `compensations()`,
`plan()`, `step(StepId)`.

### 3.2 Construction

```rust
pub struct PartialCommitBuilder { .. }
impl PartialCommitBuilder {
    pub fn new(primary: PrimaryLeg) -> Self;                       // primary gets StepId(0)
    pub fn committed(&mut self, effect: StepEffect, targets: StepTargets, replay: StepReplay) -> StepId;
    pub fn failed_sibling(&mut self, effect: Option<StepEffect>, targets: StepTargets,
                          replay: StepReplay, application: Application, error: AccountError) -> StepId;
    pub fn compensation(&mut self, effect: StepEffect, targets: StepTargets,
                        reverses: Vec<StepId>, outcome: CompensationOutcome) -> StepId;
}
impl AccountErrorBuilder {
    #[must_use] pub fn partial_commit(self, record: PartialCommitBuilder) -> Self;
}
impl AccountError {
    pub fn partial_commit(&self) -> Option<&PartialCommit>;
    pub fn leg_recovery(&self) -> &RecoveryClass;           // == recovery() when no record
    pub fn throttle_observations(&self) -> Vec<(&AccountError, &RecoveryClass)>;
}
```

`partial_commit` is legal on a fresh builder and on a builder from
`into_builder` (Google and IMAP attach a record to an already classified leaf
error). It does not change kind, cause chain, classification prefix,
`idempotency_override`, `throttle_scope` or budgets. `into_builder` preserves
an attached record. A record is attached EXACTLY ONCE, by the code that owns
the public `Account` operation's replay judgment: for a direct write, the
public method itself; for a bulk door, the function that settles that door's
item lanes (IMAP: `run_folder_mutation` for `BulkMove`,
`run_destroy_mutation_groups` for `BulkDestroy`, `run_patch_mutation_group`
for two-sided flag patches). Generic mapping and transport helpers
(`account_error_with`, `into_account_error`, `submit_write_batch_resource`,
the JMAP chunk loops, DAV `relocate_*`, IMAP connection methods, IMAP
`copy_messages` / `delete_messages` / `set_flag`) return crate-local typed
progress (section 5), never a record; the owner declares every replay
judgment relative to the public operation. There is no merge rule.

Public operations that compose other public operations never call the
attaching public form. Each composed operation is split into a crate-local
inner form that returns typed progress and a public wrapper that attaches;
the composing operation calls the inner forms and attaches once over all
legs. Named instances: IMAP `move_thread` and `delete_thread` (5.5). An
operation that forwards a single public call unchanged and attaches nothing
itself (Graph `move_thread` -> `add_to_container`) needs no split: the
forwarded error is already the operation's record, and the replay judgments
of the forwarded operation are the forwarding operation's.

Every effect-bearing leg's `Application` (primary and failed siblings) is
`Application::from_leaf(&leaf)`. A producer may raise `NotApplied` to
`Uncertain` only with a reason stated in section 5 (for example Google's
successful session status with an unusable `Location`) and only when the leaf
carries no explicit `Unsent` attempt; it never lowers. Hand-declared
`NotApplied` does not exist. Together with 3.3 this makes record validation
total over every leaf a mapper can build: `try_build().expect(..)` stays the
producer contract, and no runtime server answer can reach a panic through a
record. Compensation outcomes likewise come from
`CompensationOutcome::from_error`, with the one stated override.

A producer attaches a record only when it would pass 3.3: when the only fact
is a single `NotApplied` primary with nothing committed, no sibling and no
compensation (a first-chunk refusal with nothing else, a `draft_send` proven
refusal), it returns the plain leaf.

Ids an operation never attempted (later folders after an IMAP folder failure,
later chunks after a JMAP or Graph chunk failure) appear nowhere in the
record. A record describes effects and failed attempts; an unattempted id has
neither, and a replay of the public operation re-resolves its targets.

`throttle_observations` returns `(self, leg_recovery())`, then
`(sibling_error, sibling_error.recovery())` for every failed sibling, then
`(compensation_error, compensation_error.recovery())` for every Failed or
Unconfirmed compensation. Without a record it returns `[(self, recovery())]`.

### 3.3 Validation in `try_build`

New `AccountErrorBuildError` variants, all producer-bug surfaces:

- `PartialCommitAlreadyAttached` - `partial_commit()` was called on a builder
  that already carries a record (a fresh builder called twice, or an
  `into_builder` of an error that has one). Rebuilding a recorded error for
  decoration is legal.
- `PartialCommitEmpty` - no committed step, no sibling, no compensation, and the
  primary is not `Uncertain`.
- `PartialCommitApplication { step: StepId }` - any of: `effect` is `None` and
  application is not `NotApplicable`, or `effect` is `Some` and application is
  `NotApplicable`; an effect-bearing leg declared `NotApplied` while
  `Application::from_leaf` gives `Uncertain` (explicit `InFlight`, or leaf
  recovery `Reconcile`); an effect-bearing leg declared `Uncertain` while
  `from_leaf` gives `NotApplied` and the prefix carries an explicit `Unsent`
  attempt; a `Committed` step with `effect: None`. Absent attempt evidence
  constrains nothing (absent is not `Unsent`). The rule is stated in terms of
  `from_leaf` so the helper and validation cannot drift: a leg declared with
  the helper's value always passes. Validation reads only each error's
  classification prefix (a new `pub(crate) fn classification_attempt()` on
  `AccountError`), never decoration.
- `PartialCommitCompensation { compensation: StepId }` - `reverses` empty;
  a reference that does not resolve; a reference to a compensation, to an
  effectless leg, or to a `NotApplied` leg; a step named by two compensations;
  `Failed` whose error prefix carries `InFlight`; `Unconfirmed` whose error
  prefix carries explicit `Unsent`.
- `PartialCommitNested { step: StepId }` - a sibling or compensation error
  itself carries a record.

### 3.4 Plan derivation (pure, in `try_build`, after the leaf recovery)

Legs are the primary (with `leg_recovery`) and every `Failed` sibling (with its
own `recovery()`), in id order. A step or leg is *resolved* when a `Succeeded`
compensation names it. Its historical application/outcome is never rewritten.

Per leg with recovery `r`:

1. `Engine(d)`: push into `directives`, deduplicating only identical values;
   append the leg id to that entry's `origins`.
2. terminal: push `(id, r)` into `barriers`.
3. `Retry(a)`: `AfterAuthRefresh` sets `prerequisites.auth_refresh`;
   `AfterStateRefresh` sets `prerequisites.state_refresh`.
4. `Reconcile(a)`: if `a.reason == InternalFailureAfterSend`, push
   `(id, InternalFailure)` into `barriers` (never retry or reassert broken
   client code). If the leg is effect-bearing and unresolved, push its id into
   `gating` and union `a.guidance.actions` minus `CheckTarget` into
   `leg_actions`. An effectless or resolved Reconcile leg contributes nothing
   else.

Per effect-bearing leg with application `Uncertain`, unresolved, and not
already gating: `Blocking` -> `gating`; `Repeatable` -> `residual`.

Per unresolved `Committed` step with `Blocking` whose targets are incomplete
(`complete == false`): push into `gating`. The effect certainly happened but
its identity is not fully known, so observation is needed to learn it. This is
how a create that was acknowledged without a usable id is represented
truthfully (committed, unidentified) rather than as an `Uncertain` leg.

Per compensation `Unconfirmed`: each named step that is not otherwise resolved
goes to `gating` if `Blocking`, else `residual`. Compensation errors never feed
`directives`, `barriers` or `prerequisites` (a failed orphan cleanup must not
override a proven submission refusal); they do feed throttle observations.

*Outstanding blocking* = any unresolved `Committed` step with `Blocking`. An
uncertain `Blocking` leg is not outstanding blocking: observation decides it
(observed applied -> do not replay; conclusively observed not applied -> this
leg no longer prevents replay), which is exactly `AfterObservation`. A failed,
incomplete or indeterminate check never clears a gate; replay requires every
gate to permit it and all other plan constraints to be met. A committed
`Blocking` step cannot be observed away: it happened.

Adjudication basis (2026-10-25): GOOG-027 in `notes/bugs-deferred.md` requires
"Both uncertain send outcomes require checking before another send." That
settles uncertainty as an observation gate; it does not impose a permanent
replay ban after non-application is established. This also follows section
11's conceded rule that observe-first must not project to permanent
protection. In `crates/types/src/error/recovery.rs`,
`transient_retry_or_reconcile` already gives an in-flight non-idempotent send
`CheckTarget` reconciliation, while `derive_internal` separately identifies
`InternalFailureAfterSend`. Adding a record for a `Repeatable` draft must not
turn the former into a permanent ban. Section 5.3 therefore keeps its Graph
check-then-possibly-resend rule. Committed `Blocking` effects and barriers
(including `InternalFailureAfterSend` and JMAP's unreadable-result protocol
barriers) still force `Never`. `AfterObservation` supplies conditional safety,
not an observation implementation or an executable continuation.

`may_duplicate` = any unresolved step or leg whose effect is `Created` or
`Submitted`, whose replay is `Blocking`, and which is `Committed` or
`Uncertain`. It is read from the record, never from
`AccountOperation::is_idempotent()`: the table cannot tell a duplicable create
from a move or a destroy, for which `DedupeByClientId` has no meaning. The plan
reads no `AccountOperation` at all; the outer error is the primary leaf and
keeps the leaf's own operation (relevant for JMAP thread operations whose two
legs carry `BulkMove` and `RemoveFromContainer`).

`replay` (defined as safety ONCE every prerequisite is met and every directive
performed; outstanding prerequisites and directives suspend execution by
definition, and performing them never lifts `Never`):
`Never` if outstanding blocking or `barriers` non-empty; else
`AfterObservation` if `gating` non-empty; else `Safe`.

### 3.5 `recovery()` is the operation recovery

With no record, `recovery() == leg_recovery()` and every existing error is
unchanged. With a record, `recovery()` is a SAFE scalar projection of the plan:
it never authorizes execution the plan forbids, and it drops an observation
the scalar could express only where rule 1 lets an auth or permission class
outrank it (below). It is exact for a single-leg sequential record, which is
the only shape a bulk lane may carry (section 4.1). First match wins:

1. The first barrier (id order) in `AuthLost | NeedsAdminConsent |
   NeedsPolicyChange | NoPermission` -> that class. These outrank gating on
   purpose: they carry user remediation (`suggest` maps them to reauthorize,
   admin consent or a policy change) that no observation can stand in for,
   and for the auth classes observation is impossible until the user acts. A
   terminal class authorizes no execution, so the safety half of the
   guarantee holds; the gating the scalar cannot show stays in
   `plan().gating` and the record (the Graph three-target test in 6.3 pins
   `NoPermission` with C gating).
2. The first planned directive -> `Engine(d)`. A consumer that acts on
   directives must act on every entry of `plan().directives`. Performing a
   directive never authorizes resubmitting the original operation: when
   `plan().replay` is `Never`, the operation must not be replayed after the
   directive either, and when it is `AfterObservation` the observation still
   comes first. The bulk engine does not requeue a `BlockedByEngine` id
   within the campaign (`queue_unresolved_for_retry`, `retry_candidates`);
   the rule binds downstream consumers of direct writes and any future
   engine path that resumes after a directive.
3. `gating` non-empty -> `Reconcile { reason, guidance: CheckTarget + leg_actions
   + DedupeByClientId when plan.may_duplicate + ObserveOnly when replay is
   Never, retry_hint, throttle_scope }`, where `reason` is `PartialCommit` when
   an unresolved `Committed` step with `Blocking` exists (a `Repeatable`
   committed step, such as an orphan draft or an upload session, is blindly
   replayable by definition and does not qualify), else the first gating
   leg's own Reconcile reason, else `PartialCompletionSignal`; `retry_hint` and
   `throttle_scope` are the primary leg's own advice values when it is Retry
   or Reconcile, else `None`.
4. The first remaining barrier -> that class.
5. Otherwise, `Never` (which here can only come from a committed `Blocking`
   step) -> `Reconcile { PartialCommit, CheckTarget + ObserveOnly +
   DedupeByClientId when plan.may_duplicate, primary hint and scope }`;
   `Safe` -> `Retry { disposition: AfterStateRefresh if
   prerequisites.state_refresh, else AfterAuthRefresh if
   prerequisites.auth_refresh, else SameRequest; reason: the primary leaf's
   RetryReason when it is Retry, else derived from the primary leaf KIND
   (`Server(RateLimited)` -> `RateLimited`, `Server(QuotaExhausted)` ->
   `QuotaExhausted`, `Server(Unavailable | Error)` -> `ServerUnavailable`,
   anything else -> `Transport`); primary hint and scope }`.

Why `AfterStateRefresh` dominates in the scalar: on built-in request paths a
retry re-enters the auth lifecycle that produced `RefreshTransient` (net calls
`source.current()` before every dispatch), and bulk performs no separate auth
step for `AfterAuthRefresh`, while a state refresh is a real pre-execution step
(bulk routes it to read-back). The plan keeps both prerequisites for
downstream `Account` implementations; the scalar does not promise that a retry
fulfills an auth prerequisite universally.

`leg_recovery()` is the primary failed leg's advice, whole (disposition and
reason included). It applies to that leg, re-addressed at the leg's targets
(roles name a changed identity, e.g. the destination event after a Google
move); it is evidence, not an executable continuation. Terminal `recovery()`
means no automatic execution recovery remains, never that nothing happened:
standing effects are in the record.

New vocabulary in `recovery.rs`:

- `ReconcileReason::PartialCommit` - an earlier step committed and the
  operation cannot be blindly replayed.
- `ReconcileAction::ObserveOnly` - observation does not authorize resubmitting
  the original operation. Every producible reconcile still carries
  `CheckTarget`; extend `every_producible_reconcile_requests_check_target`.

`suggest` is unchanged and runs on the operation recovery (so a protected
reconcile from a throttled leg suggests no `RetryLater`, matching today's
`ThrottledMidFlight`). `Fatal` keeps validating `recovery()`; document that a
`Fatal` may carry a record.

A projected `PartialCommit` reconcile from a throttled leg keeps the leg's
hint, scope and budgets but not its reason. `record_repair_throttle`
(`crates/sync/src/recovery.rs`) treats `ReconcileReason::ThrottledMidFlight`
as a throttle signal on its own, so a throttled leg with no hint, no scope and
no budget loses that signal once projected. Repair paths are reads and carry
no record today, so this is latent; the error-model section says so, and that
a consumer wanting the throttle fact reads `throttle_observations()`, never
the reconcile reason.

### 3.6 Exports and telemetry

- `TelemetryView` gains `partial_commit: Option<PartialCommitTelemetry>`:
  `{ committed: Vec<(StepEffect, u32)>` (one entry per distinct effect, in
  first-seen order, with a saturating count: JMAP records one step per
  confirmed id, so a per-step list would scale with the target set and
  allocate per `telemetry_fields()` call), `primary_application: Application,
  sibling_count: u32` (saturating), `compensations: Vec<(&'static str, u32)>`
  ("succeeded"|"failed"|"unconfirmed", saturating counts), `replay:
  ReplaySafety, leg_recovery: &'static str (recovery_discriminant of
  leg_recovery) }`. No targets, no text. Update the field list in
  `telemetry_has_no_free_form_text`.
- `SupportExportConsented` gains `partial_commit: Option<PartialCommitExport>`:
  every step's id, effect, role-tagged targets, completeness, replay,
  application and outcome discriminant; compensation ids, `reverses`, outcome.
  No error text.
- `SupportExportInternal` gains `partial_commit_chains: Vec<(StepId,
  Vec<CauseSummary>)>` for sibling and compensation errors.
- `Display` and `message_key` are unchanged (leaf kind).

### 3.7 Scopes and the ledger

`ErrorScope` gains `Submission { id: ObjectId }`, `CloudFile { id: String }`
and `UploadSession` (no field: a pre-authenticated upload URL is a credential
and never enters scope or diagnostic text). `scope_fields` arms: `("kind",
"submission"), ("id", ..)`; `("kind", "cloud_file"), ("id", ..)`; `("kind",
"upload_session")`. Extend `scope_fields_pin_the_serialized_shape`.

`ledger_envelope.rs`: tags 11, 12, 13 for the three variants, gated
`reader.version >= 7`; bump the ledger envelope version to 7 with a module-doc
paragraph in the existing version list. The ledger never persists a record:
add "the partial-commit record" to the codec doc's "What is NOT preserved"
paragraph with the reason (ledger rows are sync coverage/repair failures; a
restored error is evidence, never retried through, and attaching a record to a
digest's synthesized cause would validate invented evidence).

## 4. Consumer adoption (`bifrost-sync`)

### 4.1 Lane rule for bulk producers

A bulk item or termination error never carries failed siblings: bulk items are
per target. Producers pin this in their tests (section 6). A bulk item lane is
`Uncertain` when the primary application is `Uncertain`, else `Failed`.

### 4.2 `bulk.rs`

- `classify_item_outcome`, `uncertain_origin`, `reconcile_origin` keep
  dispatching on `recovery()`. `ObserveOnly` makes `reconcile_origin` return
  `Protected` with no code change (not every action is `CheckTarget`); add a
  doc line saying so and naming `ObserveOnly`.
- `record_error_throttle(throttles, account_id, error)` iterates
  `error.throttle_observations()` and records each `(source, recovery)` by its
  own class: `Retry` -> `record_throttle(.., advice, source)`, `Reconcile` ->
  `record_reconcile_throttle(.., advice, source)`, others nothing. The
  `Terminated` arm's two direct record calls are replaced by one
  `record_error_throttle(&slot.throttles, account_id, &original)`, so a sibling
  or compensation 429 is held under its own scope and budgets even when the
  operation recovery is terminal or Engine. For a `Failed` item the primary's
  hold is recorded twice, once inside `classify_item_outcome` and once by the
  Batch arm's `record_error_throttle` (true today as well); deadlines take the
  max, so the duplicate is harmless. Say so in a comment so it is not "fixed"
  into a gap by removing the wrong call.
- Partial-commit accounting: every item error (Failed and Uncertain lanes) and
  every `Terminated` error whose `partial_commit()` is `Some` is pushed, as
  observed, into a campaign `Vec<PartialCommitObservation>`; item errors carry
  their item's `ObjectId`, termination errors carry `None` (no fan-out to
  remaining targets). Nothing ever removes an entry: requested-state
  satisfaction and residual effects are separate predicates and the engine
  observes only the requested state.

### 4.3 `mutation/mod.rs`

```rust
#[derive(Debug, Clone)]
pub struct PartialCommitObservation { pub target: Option<bifrost_types::ObjectId>, pub error: AccountError }
pub struct MutationCounters { .., pub partial_commit_observations: Vec<PartialCommitObservation> }
```

Doc: entries are observations, not unique effects; one effect reported on two
attempts appears twice. Update the module doc bullet list.

### 4.4 Passthrough

No code change: direct writes forward the error, and consumers read
`recovery()`, `leg_recovery()` and `partial_commit()`. Update the passthrough
cluster comments ("bypass the idempotency / read-back / recovery pipeline")
to say the forwarded error's `recovery()` is the operation recovery.

## 5. Producer adoption

Each brick deletes the old helper named in 2.3 and the comments describing it.
"Leaf" below always means the failed request's own `AccountError` exactly as
the crate's mapper builds it today (kind, chain, evidence, override, throttle
scope, budgets untouched).

### 5.1 CalDAV and CardDAV

- `relocate_event` / `relocate_contact` return a crate-local
  `Result<(), RelocateFailure>`: `enum RelocateFailure { Before(AccountError),
  MovedThenPut { destination: String, error: AccountError },
  CopiedThenDelete { destination: String, source: String, error: AccountError } }`.
- `event_update` / `contact_update` map it at the boundary:
  `MovedThenPut` -> leaf + record: primary `{ effect: Updated, targets:
  [Subject: Event/Contact(destination id)] complete, replay: Repeatable,
  application: Application::from_leaf(&leaf) }`, committed `Moved` with `[Source: old id,
  Destination: new id]` complete, `Blocking` (replay MOVEs from a source that no
  longer exists). `CopiedThenDelete` -> primary `{ Deleted, [Subject: source
  id], Repeatable, from_leaf }`, committed `Copied { [Source, Destination] }`,
  `Blocking` (replay's `If-None-Match` PUT is refused at the destination).
  Every DAV primary below likewise takes its application from `from_leaf`.
  The leaf's `ErrorScope` becomes the id where the leg ran. The restated
  `idempotency_override(false)` disappears with `partial_sequence_error`;
  `recovery()` comes from the plan.
- `event_rsvp`: committed `Submitted { [Subject: Event(id)] }`, `Blocking`
  (a replay re-sends the iTIP REPLY to the organizer). The primary is the PUT
  (`Updated`, `Repeatable`) or, for the local encode failures between POST and
  PUT, an effectless leg (`effect: None`, `NotApplicable`) carrying the local
  `Unsupported` / `Internal` / `Request` leaf. Result: a local refusal after the
  POST is terminal with the `Submitted` record (it was a `Reconcile` before:
  the organizer already acted, nothing can complete the local copy
  automatically, and the record says so); a retryable PUT failure is a
  protected `PartialCommit` reconcile `[CheckTarget, DedupeByClientId,
  ObserveOnly]` with the PUT's hint. `DedupeByClientId` appears because the
  committed `Submitted` step is a duplicable submission (`may_duplicate`, 3.4),
  even though `EventRsvp` is idempotent in the operation table.
- DAV relocation results: the committed `Moved` / `Copied` steps are
  `Blocking` with complete targets, so a retryable leg is a protected
  `PartialCommit` reconcile `[CheckTarget, ObserveOnly]` with the leg's hint
  and scope (no dedupe: neither is a create), a terminal leg stays terminal
  with the record, and an auth or permission leg keeps its class (rule 1).

### 5.2 Google

Every effect-bearing Google leg takes its application from
`Application::from_leaf`; the one stated raise is the session-create case
below.

- `calendar::update`: on PATCH failure after a move, attach to the leaf from
  `event_error`: primary `{ Updated, [Subject: Event(destination)], Repeatable }`,
  committed `Moved { [Source: Event(original composite id), Destination:
  Event(destination composite id)] }`, `Blocking`. Keep the destination
  `ErrorScope`. Delete `event_move_patch_error` and its user-safe continuation
  text: the record carries the target, `leg_recovery()` the PATCH advice.
- `contacts::update`: on photo failure after fields: committed `Updated {
  [Subject: Contact(id)] }`, `Repeatable` (an unversioned replay re-fetches
  source metadata and re-applies values; versioned photo patches are refused
  before I/O); primary `{ Updated, [Subject: Contact(id)], Repeatable }`.
  `recovery()` equals the photo leg's recovery. Delete
  `contact_fields_photo_error`.
- `cloud.rs`: `upload_and_link` returns a crate-local progress struct instead
  of `(Error, SessionDisposition)`: `{ error, session: SessionState, file:
  Option<FileState>, permission_created: bool }`. `create_upload_session`
  failure: primary `{ Created, [Subject: UploadSession] incomplete, Repeatable
  }`, application `from_leaf`, raised to `Uncertain` (stated reason: the
  session may exist) for a successful status with a missing or unusable
  `Location`; an unreadable answer or `InFlight` drop is already `Uncertain`
  by `from_leaf`. A `NotApplied` session-create failure is a lone primary and
  returns the plain leaf (3.2). Chunk failure: committed
  `Created(UploadSession)` Repeatable; the primary depends on which chunk
  failed. A non-final chunk cannot create the file, so its primary is `{
  Updated, [Subject: UploadSession], Repeatable }` (bytes appended to the
  session); the final chunk's primary is `{ Created, [Subject: CloudFile]
  incomplete, Repeatable }`; both take `from_leaf`. The cancel is
  `compensation(Deleted, [UploadSession], [session step], outcome)` where
  `cancel_upload_session` now returns `Result<(), Option<Error>>`, mapped
  with the same context and `CompensationOutcome::from_error`. Its two
  outcomes that today produce no error value: a session URL that fails
  `is_drive_upload_url` sends nothing, so it returns `Err(None)` and the
  record carries NO compensation (nothing ran; the session step stands, as
  for Graph's admission refusal); an unfollowed 3xx `Ok(response)` is a real
  answer that did not accept the cancel, mapped through `response_error`
  into an `Acknowledged` leaf, so `Failed`. `404 | 410 | 499` stay
  `Succeeded`. Permission failure: `create_sharing_permission` is two
  requests and is split at its boundary. A permission POST failure: committed
  `Created { [Subject: CloudFile(file_id)] complete }` Repeatable (an
  unreferenced file); primary `{ Created, [Subject: CloudFile(file_id)],
  Repeatable, from_leaf }` (the permission). A `webViewLink` GET failure after
  a successful POST: committed file as above and committed `Created {
  [Subject: CloudFile(file_id)] complete }` Repeatable (the permission, on a
  file a replay will not reference); primary effectless (`PrimaryLeg::effectless`)
  carrying the GET leaf, so an unsent GET can never describe the permission as
  `NotApplied`. Delete `ABANDONED_SESSION_TEXT` and the `SessionDisposition`
  decoration. Resulting classes: every step is `Repeatable`, no compensation
  feeds the plan, and `may_duplicate` is false, so `recovery()` equals
  `leg_recovery()` in every arm: Retry leaves project to the same Retry (same
  disposition, reason, hint, scope), a reconciling leaf (an `InFlight` drop,
  `Reconcile(TransportDropAfterSend, [CheckTarget])`) projects to the same
  reconcile with no added `DedupeByClientId`, terminal leaves stay terminal.
  The record adds the standing session, file and permission facts.

### 5.3 Graph

Every effect-bearing Graph leg takes its application from
`Application::from_leaf`, except a batch sub-request in the uncertain lane or
missing from the response, which is `Uncertain` (stated reason: no answer for
that sub-request was read).

- `send.rs` `send_message`: progress `{ draft: Option<DraftHandle>, stamped:
  bool }`. Draft create failure: no record (single leg), except a 2xx whose
  body lacks an id (`create_draft_message`'s missing-id arm): committed
  `Created { [Subject: Message] unknown }` Repeatable (an orphan draft that
  nothing sent) with an effectless primary carrying the protocol leaf; the
  barrier keeps `recovery()` the leaf's terminal class and the record names
  the orphan. Stamp failure: committed `Created { [Subject: Message(draft)] }`
  Repeatable (orphan draft); primary `{ Updated, [Subject: Message(draft)],
  Repeatable }`. Send failure: committed draft (and stamp `Updated`,
  Repeatable); primary `{ Submitted, [Subject: Message(draft)], Blocking }`,
  application `from_leaf` (`InFlight` -> Uncertain -> gating, no committed
  `Blocking` step, so `AfterObservation`: `Reconcile { TransportDropAfterSend,
  [CheckTarget, DedupeByClientId] }`, check then possibly resend; proven
  refusal -> `NotApplied` -> leaf recovery preserved). Map every leg's error
  with `AccountOperation::Send` (today the draft leg is stamped `DraftCreate`).
  A refused retryable send leaves its orphan draft and an automatic retry
  creates another; that is the accepted `Repeatable` cost (3.1), bounded by
  the retry budget, and the missing cleanup is filed (section 9).
  `send_raw_message`: committed `Created(draft)` from the MIME import, primary
  as above; an import 2xx without an id is committed `Created { [Subject:
  Message] unknown }` `Repeatable` (nothing was sent, so a replay leaves only
  another orphan) with an effectless primary carrying the protocol leaf.
- `pim/messages.rs` `submit_write_batch_resource`: drain every subresponse AND
  the missing-id sweep and return a crate-local `BatchProgress { committed:
  Vec<(usize, Option<ObjectId>)>, failed: Vec<(usize, Application,
  AccountError)> }` instead of the first failure. The `Option<ObjectId>` is the
  new identity a successful sub-request returned, read from the subresponse
  body's `id` and re-qualified with the source message's owner for a shared
  mailbox; it is filled only for moves (Graph assigns the moved message a new
  id) and is `None` when a move success lacks a usable id. The public methods
  (`set_is_read`, `set_importance`, `set_category`, `set_extended_property`,
  the move and thread operations in `pim/messages.rs` and `pim/threads.rs`)
  convert it through one crate-local `batch_progress_error(progress, targets,
  effect, replay, operation)` call that they make themselves, passing the
  effect and replay judgment for their own operation: the primary is the first
  failure in submission order (not response order); every other failed or
  missing sub-request is a `failed_sibling` with its own error; every 2xx (and
  destroy-404) sub-request is a `committed` step. Judgments: patch -> `Updated`
  Repeatable (each call refetches the etag); destroy -> `Deleted` Repeatable
  (404 is success); move -> `Moved` Blocking (Graph assigns a new id), with
  targets `complete([Source: Message(old id), Destination: Message(new id)])`
  when the new id is known, else `at_least([Source: Message(old id)])`, which
  gates (3.4). A lone failure with nothing committed returns the plain leaf
  (3.2).
- `pim/threads.rs` `delete_thread` trash branch: the chunk loop accumulates
  one `BatchProgress` across every chunk it sent (sub-request indexes
  offset by the chunk start) and converts once, so committed earlier chunks
  are reported; the loop still stops at the first chunk with a failure, and
  unsent later chunks are absent (3.2). A whole-batch failure of a later
  chunk (the `$batch` POST itself) is the primary, with every earlier
  committed id as a step. `move_thread` forwards `add_to_container` unchanged
  and attaches nothing (3.2).
- `cloud.rs`: as Google: `create_upload_session` success makes the session a
  committed `Created(UploadSession)` Repeatable before `admit_upload_url`; an
  admission refusal is an effectless primary (local `Protocol` leaf) with the
  session step standing and no compensation (the URL is untrusted, so no
  cancel is sent); `cancel_upload_session` returns its outcome as a
  compensation on the session step only (outcome from
  `CompensationOutcome::from_error`); a non-final chunk failure is an
  `Updated(UploadSession)` primary as in Google; an `InFlight` final chunk is an
  `Uncertain` `Created(CloudFile)` primary that the cancel does NOT reverse;
  createLink failure: committed `Created(CloudFile(item_id))`.

### 5.4 JMAP

- Send boundaries (`send_message`, `send_raw_message`, `draft_send`):
  - Whole-request transport failure from `batch.send()` /
    `request.send()`: `convert_transport` gains a send-boundary context flag
    so a custom transport error with no net evidence is stamped
    `Attempt(InFlight)` for these three call sites only; the result is a
    single-leg leaf (no record needed: `Transport + InFlight + Send` already
    reconciles).
  - `ResponseDecode` from the envelope: leaf kept (`Protocol(ParseFailed)`),
    `push_cause(Attempt(Acknowledged))` on a fresh build, record with primary `{
    Submitted, [Subject: Message] incomplete, Blocking, Uncertain }`
    (`from_leaf` gives `NotApplied`; raised, stated reason: an unreadable
    received response proves nothing about the submission, and the leaf
    carries no `Unsent`). Plan: gating, and the `ParseFailed` barrier makes
    replay `Never` -> `Reconcile { PartialCompletionSignal, [CheckTarget,
    DedupeByClientId, ObserveOnly] }`.
  - Missing or malformed submission result (`submission_create_error` non-
    rejected arm, and `draft_send`'s extraction): leaf from `to_acct_err`, same
    raised `Uncertain` `Submitted` primary. For `send_message` /
    `send_raw_message`, when the Email create/import result yields a usable id
    (the code already reads that result on this path to advance state), add
    committed `Created { [Subject: Message(email id)] }` Repeatable: the Email
    is deliberately retained and is a known earlier commit.
  - `serverPartialFail` on the submission (`Error::Method` whose type is
    `MethodErrorType::ServerPartialFail`): NOT a proven refusal. RFC 8620
    section 3.6.2 makes it the method error after which server state may have
    changed, and the mapper builds `Protocol(PartialResponse)`, which on
    `Send` derives `Reconcile`, so `from_leaf` gives `Uncertain`. It takes the
    missing-result arm above: `Uncertain` `Submitted` primary, committed
    `Created` when the Email id is readable, Email retained, NO compensation.
    `submission_create_error`'s `rejected` predicate excludes it. Today's
    code destroys an ambiguously submitted Email on this answer, which the
    ruling forbids, so the predicate change is in scope even though the
    defect predates this spec.
  - Proven refusal (`Error::Set` from the submission's `notCreated`, or
    `Error::Method` of any type except `ServerPartialFail`): primary
    `Submitted` Blocking with `from_leaf` (`NotApplied`); for `send_message` /
    `send_raw_message` with a created Email, committed `Created { [Subject:
    Message(email id)] }` Repeatable and `compensation(Deleted, [Message(email
    id)], [create step], outcome)` from `discard_created_send_email`, which now
    returns `Result<(), AccountError>`: Ok -> `Succeeded`; otherwise
    `CompensationOutcome::from_error` (`InFlight`/absent -> `Unconfirmed`, a
    proven `notDestroyed` -> `Failed`), with the stated override that a decoded
    response missing the `destroyed(id)` result is `Unconfirmed`. The Email is
    destroyed only on proven refusal, only when this call created it; never a
    caller draft (`draft_send` never compensates).
  - Submission acknowledged but Email create/import id missing: committed
    `Submitted { [Subject: Submission(id)] complete when the id is known, else
    unknown }` Blocking; committed `Created { [Subject: Message] unknown }`
    Blocking (the Email certainly exists: the submission resolved its `#ref`
    to it; `Blocking` because a replay sends again while it is unidentified);
    primary effectless (`PrimaryLeg::effectless`) carrying the extraction leaf
    (`Protocol(MissingField)`, a barrier). The committed incomplete `Blocking`
    create gates (3.4), so rule 3 fires before the barrier's rule 4 and the
    result stays check-before-resend: `Reconcile { PartialCommit,
    [CheckTarget, DedupeByClientId, ObserveOnly] }`. The record tells the
    truth (committed, unidentified) instead of declaring a known commit
    `Uncertain` to steer the projection.
  - Scheduled send with acknowledged submission but no usable submission id:
    committed `Created(Message(email id))` Repeatable; committed `Submitted {
    [Subject: Submission] unknown }` Blocking (gates, 3.4); primary effectless
    carrying the extraction leaf. Result: `Reconcile { PartialCommit,
    [CheckTarget, DedupeByClientId, ObserveOnly] }`.
- `draft_create` without a usable id: committed `Created { [Subject: Message]
  unknown }` Blocking (the `created` entry is present, so the draft exists; a
  replay creates a visible duplicate of the operation's intended effect) and
  an effectless primary carrying the extraction leaf. Result: `Reconcile {
  PartialCommit, [CheckTarget, DedupeByClientId, ObserveOnly] }`.
- `reschedule_send`: replacement create failure: committed `Cancelled {
  [Subject: Submission(old id)] }` Blocking; primary `{ Submitted, [Subject:
  Submission] incomplete, Blocking }` with application `from_leaf`
  (`NotApplied` for a `notCreated` refusal, `Uncertain` for transport drop
  and `serverPartialFail`; an undecodable or missing result is raised to
  `Uncertain`, stated reason as for the send boundary). A terminal proven
  refusal stays terminal with the `Cancelled` record (was reconcile); a
  retryable proven refusal (`rateLimit`, `overQuota`) becomes a protected
  `PartialCommit` reconcile `[CheckTarget, ObserveOnly]` with its hint and
  scope (the cancellation is Blocking; a refused create may not duplicate, so
  no `DedupeByClientId`); an uncertain create reconciles with
  `[CheckTarget, DedupeByClientId, ObserveOnly]`.
  Ordering and the cancel-before-create gate are unchanged.
- Chunked `Email/set` (`set_keyword`, `patch_mailbox_membership_of`,
  `destroy_thread_emails_if_unchanged`): return a crate-local
  `SetProgress { committed: Vec<EmailId>, failed: Vec<FailedSet>,
  after_chunk: Option<crate::Error> }` where `FailedSet { ids: Vec<EmailId>,
  application: Application, error: crate::Error }`, draining EVERY id of the
  failing chunk (`updated`/`destroyed`/not-* lanes, missing ids `Uncertain`)
  instead of stopping at the first. Each per-id failure is a `FailedSet` of
  one id. A whole-call failure on a chunk is ONE `FailedSet` holding every id
  of that chunk with the call error (one leg whose targets list the ids, never
  N copies of one error), so a whole-call failure of the first chunk with
  nothing confirmed is a lone primary and returns the plain leaf (3.2). Ids of
  chunks never sent are absent (3.2); there is no `unsent` field.
  `after_chunk` carries a failure raised after a chunk whose ids all
  confirmed (the guarded destroy's missing `newState` before the next chunk);
  it becomes an effectless primary with every confirmed id committed. Public
  operations attach: primary = `after_chunk` when present, else the first
  failed set in input order; siblings = the remaining failed sets; committed =
  every confirmed id; per the 3.2 emptiness rule, a lone refused set with
  nothing confirmed returns the plain leaf.
  Keyword and membership patches: `Updated` Repeatable. Guarded thread
  destroy: `Deleted` Repeatable. The public `delete_thread` re-resolves the
  thread (`resolve_target`), re-reads membership and a fresh Email state
  (`resolve_thread_emails_in_mailbox`) and builds a new `ifInState` before
  every attempt, and an already destroyed member comes back `notFound` and is
  skipped, so replay of the public operation converges; the refused
  `ifInState` belongs to the internal request, not to the public replay.
  Behavior change, intended: the guarded destroy's `after_chunk` (missing
  `newState`) was `Reconcile` via `partial_email_set_error`; it is now the
  effectless primary's `Protocol(ParseFailed)` barrier over `Repeatable`
  committed destroys, so `recovery()` is terminal `ProviderContractViolation`
  (rule 4) with every destroyed id in the record. A provider that omits a
  required `newState` broke its contract, and bifrost does not retry through
  contract violations; nothing is lost because the effects are recorded. A
  later-chunk retryable refusal is now a plain `Retry` (or
  `Retry(AfterStateRefresh)` for a state mismatch) instead of a reconcile.
  Delete `partial_email_set_error`.
- `move_thread` and the trash branch of `delete_thread`: the outer operation
  attaches one record over both legs: add-leg ids confirmed are committed
  `Updated` (membership add) Repeatable; remove-leg failures as above,
  `Updated` Repeatable. JMAP path patches are absolute, so replay converges and
  `recovery()` is the combined leg recovery. The outer error is the primary
  leaf with its own operation (`BulkMove` for an add-leg primary,
  `RemoveFromContainer` for a remove-leg primary); the plan reads no
  operation (3.4).
- Delete `incomplete_multi_step_write`.

### 5.5 IMAP

- Connection layer (`crates/imap/src/connection/uid_ops.rs`): new crate error
  variant `Error::MoveFallbackIncomplete { copy: CopyResult, stored_deleted:
  bool, source: Box<Error> }`, raised from the UIDPLUS fallback when STORE or
  UID EXPUNGE fails after an acknowledged COPY; `attempt()`,
  `is_connection_fatal()` and `response_code()` delegate to `source`;
  `with_attempt` rebuilds around the restamped source. `is_connection_fatal`
  is a `const fn` today: drop the `const` if delegating through the `Box`
  does not compile in a const context (it has no const caller to lose).
  `Error`'s hand-written `PartialEq` ends in `_ => false`, so the new variant
  needs its own arm there (compare `copy`, `stored_deleted` and `source`), as
  well as `Display` and `std::error::Error::source()` arms returning the
  boxed source. `uid_move_messages` is public connection-level API whose
  fallback-path callers used to receive the bare `Error::No` and kin; the
  shape change is ruled fine pre-1.0 and `reference/imap.md` states it.
  Delete `after_acknowledged_side_effect` and its test
  `the_move_fallback_floor_lifts_only_unsent_evidence_it_can_carry`; the
  leaf's own evidence (including `StateChangedBeforeSend`, local
  `MissingCapability`, withdrawn-while-queued, dropped unclaimed and expired at
  admission, all `Unsent` or evidence-less) is now kept as is.
- Account mapping: `account_error_with` maps `MoveFallbackIncomplete` by
  mapping its `source` as an ordinary leaf and attaches nothing. The only
  caller of `uid_move_messages` is `run_folder_mutation` (the `BulkMove`
  door), which attaches through one crate-local
  `move_fallback_record(leaf, &copy, stored_deleted, folder, destination)`.
  IMAP has no direct-move path to the fallback: direct `move_thread` is COPY
  then STORE/EXPUNGE through `copy_messages` and `delete_messages`, covered by
  the `FolderProgress` bullet below. The record: committed
  `Copied { [Source: folder, Destination: destination folder], complete:
  false }` Blocking (COPYUID mapping, when present, adds `Message` refs for the
  destination UIDs); when `stored_deleted`, committed `MarkedDeleted` Repeatable;
  primary `{ MarkedDeleted or Deleted, [Subject: source folder], Repeatable,
  from_leaf }`. The incomplete committed `Copied` gates (3.4) and is
  outstanding blocking, so rule 3 fires before any non-auth barrier. Result:
  a retryable STORE refusal after COPY is a protected `PartialCommit`
  reconcile `[CheckTarget, ObserveOnly]` (Failed lane, read-back Protected),
  never the retry queue; a local `MissingCapability` or other terminal leaf
  after COPY is the same protected reconcile rather than a terminal that
  hides the copy; `Copied` is not a create, so no `DedupeByClientId` and no
  change to `MutationCounters::dedupe_by_client_id`.
- `mutate.rs`: `run_destroy_mutation_groups` expunge failure: committed
  `MarkedDeleted` Repeatable (IMAP STORE on an expunged UID and UID EXPUNGE of
  absent UIDs are no-ops, and the STORE's MODSEQ cache entries for those UIDs
  are already cleared, so a replay stores unguarded); primary `{ Deleted,
  Repeatable, from_leaf }`; delete `expunge_failed_after_delete_mark`.
  `run_patch_mutation_group` second STORE failure: committed `Updated` (add
  half) and primary `{ Updated, from_leaf }`; the add half's replay judgment
  splits on the kind. For `MutationKind::Flags` the add STORE ran under the
  cache's MODSEQ, which is cleared for the applied UIDs, so a replay is
  unguarded and converges: `Repeatable`. For
  `MutationKind::ConditionalFlags` the add STORE ran under the caller's
  `UNCHANGEDSINCE`; applying it bumped the MODSEQ, so a replay of the same
  caller guard gets `MODIFIED`, which `bulk.rs` turns into a caller conflict
  and `FailedTerminal`: a precondition this effect made false, so `Blocking`.
  The primary (the remove) is `Repeatable` either way (it runs unguarded).
  `mutation_error_verdict` uses section 4.1's lane rule.
- `pim.rs` direct writes: `copy_messages`, `delete_messages`, `set_flag` return
  a crate-local `FolderProgress { done: Vec<(MailboxName, Vec<u32>, StepEffect)>,
  error: crate::Error, stage }` and are wrapped by crate-local inner forms that
  also return progress: `add_to_container_inner`, `remove_from_container_inner`
  (capability check, container decode, target decode and filter, then the
  helper; a refusal before the helper is a plain pre-write error) and
  `move_thread_inner` (the two inner legs in order, one combined progress).
  The public operations attach exactly once each: `add_to_container`,
  `remove_from_container`, `set_keyword`, `set_is_read`, `set_importance`,
  `draft_discard`, `move_thread` (over `move_thread_inner`, one record across
  the add and remove legs), and `delete_thread` (over `move_thread_inner` or
  `remove_from_container_inner`, never the public forms). `draft_send` keeps
  calling the private `delete_messages` and logging its failure (section 9).
  Judgments: earlier folders committed (`Copied` Blocking for copy; `Deleted`
  Repeatable for delete; `Updated` Repeatable for flags); within the failing
  folder of `delete_messages`, an EXPUNGE failure after STORE adds
  `MarkedDeleted` Repeatable; the failing folder's own leg takes `from_leaf`.
  Preflight failures in a later folder (`checkout_for_folder`,
  `select_folder`, `selected_uidvalidity`) and its local refusals
  (`valid_uids`, `pim_operand`) are effectless primaries.
- `container_create` / `rename` / `move` / `delete`: refresh failure after the
  write attaches committed `Created` / `Moved` / `Moved` / `Deleted`, all
  Blocking (replay hits ALREADYEXISTS or a missing old name), primary effectless
  (`NotApplicable`) carrying the refresh leaf. Plan `Never` ->
  protected reconcile, or the refresh leaf's barrier.

### 5.6 Scope of `ErrorScope` targets

Producers use `Submission`, `CloudFile`, `UploadSession` (3.7) and existing
variants; IMAP folders are `ErrorScope::Cursor(CursorScope::Folder(..))`,
matching `concurrency_conflict_error`.

### 5.7 What no producer does

No producer reorders writes, adds a request, resumes a session, or performs a
second Google move. Compensations are only the cleanups that already exist
(JMAP Email destroy, Google/Graph session cancel). The one behavior change at
a cleanup boundary is the JMAP `serverPartialFail` exclusion (5.4), which
removes a destroy rather than adding one.

### 5.8 SMTP

No code change; `reference/smtp.md` gains the audit note.

## 6. Verification bricks

Every test below is hermetic (stub transports, canned transcripts, scripted
dispatch, `StubAccount`). Every regression test is ablated: revert the
production change it pins, confirm it fails, restore (record the result in the
landing commit message).

### 6.1 Types (`crates/types/src/error/partial_tests.rs`)

`brokkr test -p bifrost-types partial_commit_`

- `partial_commit_absent_keeps_leaf_recovery`
- `partial_commit_blocking_prefix_protects_a_throttled_leg` (Retry 429 with
  `RetryHint::After(37s)`, `ThrottleScope::Mailbox`, a stamped budget -> Reconcile
  `PartialCommit` with `[CheckTarget, ObserveOnly]`, same hint and scope,
  `throttle_budgets()` unchanged, `leg_recovery()` the original Retry)
- `partial_commit_repeatable_prefix_keeps_leg_retry`
- `partial_commit_local_refusal_after_blocking_prefix_is_terminal`
- `partial_commit_transport_drop_after_blocking_prefix_reconciles`
- `partial_commit_auth_and_permission_legs_stay_barriers`
- `partial_commit_unreadable_blocking_leg_requires_a_check` (ParseFailed +
  Acknowledged + Uncertain Submitted)
- `partial_commit_proven_refusal_survives_every_cleanup_outcome` (Succeeded,
  Failed, Unconfirmed; 4xx terminal and 429 retry legs)
- `partial_commit_succeeded_compensation_clears_only_named_steps`
- `partial_commit_mixed_sibling_prerequisites_keep_both_in_plan`
- `partial_commit_distinct_sibling_directives_are_all_planned`
- `partial_commit_terminal_sibling_bars_reassertion`
- `partial_commit_effectless_read_after_blocking_create_is_protected`
- `partial_commit_internal_failure_after_send_is_a_barrier`
- `partial_commit_projected_retry_keeps_quota_reason`
- `partial_commit_validation_rejects_inconsistent_records` (one case per
  build error and sub-rule in 3.3)
- `partial_commit_application_from_leaf_always_validates` (one leaf per
  `AccountErrorKind` family, crossed with absent, `Unsent`, `InFlight` and
  `Acknowledged` evidence and idempotent and non-idempotent operations,
  including `Protocol(PartialResponse)` on `Send` and an explicit-`Unsent`
  reconciling leaf: a leg declared with `from_leaf` always builds, and
  `CompensationOutcome::from_error` likewise)
- `partial_commit_uncertain_blocking_leg_allows_send_after_observation`
  (Repeatable committed draft, Uncertain Submitted Blocking leg with a
  transport `InFlight` leaf -> `AfterObservation`, `Reconcile` without
  `ObserveOnly`)
- `partial_commit_unidentified_blocking_create_gates_before_a_barrier`
  (committed `Created` Blocking with unknown targets, effectless
  `MissingField` primary -> rule 3 `PartialCommit` reconcile, not rule 4)
- `partial_commit_repeatable_commit_keeps_the_leg_reason` (Repeatable
  committed step, gating leg -> the leg's own reason, never `PartialCommit`)
- `partial_commit_dedupe_follows_the_record_not_the_table` (a `Copied` /
  `Moved` / `Deleted` Blocking record on a non-idempotent operation carries no
  `DedupeByClientId`; an uncertain or committed `Created` / `Submitted`
  Blocking leg does)
- `partial_commit_permission_barrier_outranks_gating` (rule 1 over a gating
  sibling; gating still present in `plan()`)
- `partial_commit_directive_does_not_lift_never` (Engine projection with
  `plan().replay == Never`)
- `partial_commit_step_ids_past_u16_stay_unique` (70,000 committed steps:
  every id distinct, `step(id)` resolves each, a compensation naming the last
  one validates)
- `partial_commit_survives_into_builder_and_refuses_a_second_record`
- `partial_commit_throttle_observations_cover_every_leg`
- `partial_commit_telemetry_summary_has_no_targets` (also asserts per-effect
  counts, not one entry per step)

Extended existing tests: `brokkr test -p bifrost-types every_producible_reconcile_requests_check_target`,
`brokkr test -p bifrost-types telemetry_has_no_free_form_text`,
`brokkr test -p bifrost-types scope_fields_pin_the_serialized_shape`.

### 6.2 Sync

- `brokkr test -p bifrost-sync ledger_error_scope_v7_tags_round_trip`
  (`ledger_envelope.rs` tests; a v6 reader rejects tags 11-13).
- In `crates/sync/tests/readback_guard.rs`, with a stub bulk door:
  `brokkr test -p bifrost-sync a_blocking_partial_commit_is_read_back_not_resubmitted`,
  `brokkr test -p bifrost-sync a_repeatable_partial_commit_keeps_the_retry_queue`,
  `brokkr test -p bifrost-sync partial_commit_observations_survive_a_later_plain_failure`,
  `brokkr test -p bifrost-sync a_terminal_partial_commit_keeps_its_observation`,
  `brokkr test -p bifrost-sync a_compensation_throttle_is_held_under_its_own_scope`
  (a bulk item whose record carries a throttled Unconfirmed compensation; bulk
  items never carry siblings, 4.1). The two lane tests assert the full
  guidance vector and the `dedupe_by_client_id` counter, not only the bucket,
  so the `ObserveOnly` and record-driven `DedupeByClientId` rules are pinned
  rather than incidental.
- Unit test beside `record_error_throttle` in `crates/sync/src/engine/bulk.rs`:
  `brokkr test -p bifrost-sync record_error_throttle_holds_each_leg_under_its_own_scope`
  (a direct-write-shaped error with a terminal primary, a 429 sibling scoped to
  mailbox A and a 429 compensation scoped to mailbox B: both holds recorded,
  each with its own hint and budgets).

### 6.3 Producers (one named test per adopted path; existing tests that
pinned the old shape are rewritten in place, not deleted, unless named)

- CalDAV: `brokkr test -p bifrost-caldav partial_commit_` covering
  `partial_commit_moved_then_put_refused`, `partial_commit_copied_then_delete_throttled`,
  `partial_commit_rsvp_local_refusal_after_post_is_terminal_with_record`,
  `partial_commit_rsvp_put_drop_reconciles`; rewrite
  `rsvp_second_leg_error_records_acknowledged_first_leg`.
- CardDAV: `brokkr test -p bifrost-carddav partial_commit_` (moved-then-put,
  copied-then-delete).
- Google: `brokkr test -p bifrost-google partial_commit_` plus the rewritten
  `failed_patch_after_move_keeps_its_classification_and_records_the_move`,
  `rate_limit_after_move_continues_at_destination_without_repeating_move`
  (now: operation recovery protected reconcile with the 429 hint; leg recovery
  Retry; destination target), `auth_failure_after_move_keeps_auth_recovery_and_public_continuation`,
  `photo_failure_after_field_update_preserves_permission_failure`,
  `cleanup_does_not_change_the_recovery_class`; new
  `partial_commit_upload_session_cancel_failure_is_a_failed_compensation`,
  `partial_commit_upload_session_cancel_redirect_is_a_failed_compensation`,
  `partial_commit_missing_session_location_is_uncertain`,
  `partial_commit_non_final_chunk_failure_updates_the_session`,
  `partial_commit_permission_failure_reports_the_uploaded_file`,
  `partial_commit_permission_created_then_link_read_failure` (POST ok, GET
  fails unsent: committed permission, effectless primary, never `NotApplied`
  permission).
- Graph: `brokkr test -p bifrost-graph partial_commit_`:
  `partial_commit_batch_keeps_success_refusal_and_missing_siblings` (the
  three-target move: A ok, B 403, C missing -> barrier NoPermission, C gating,
  A committed Moved with A's new id as `Destination`),
  `partial_commit_batch_move_success_without_id_gates`,
  `partial_commit_batch_reconciling_subresponse_is_uncertain` (a sub-request
  whose leaf derives `Reconcile` is declared `Uncertain` and builds without
  panic), `partial_commit_delete_thread_keeps_earlier_chunks`,
  `partial_commit_send_refusal_reports_the_orphan_draft`,
  `partial_commit_send_drop_reconciles` (asserts `[CheckTarget,
  DedupeByClientId]`, no `ObserveOnly`),
  `partial_commit_draft_create_without_id_in_send_names_the_orphan`,
  `partial_commit_upload_admission_refusal_keeps_the_session`.
- JMAP: `brokkr test -p bifrost-jmap partial_commit_`:
  `partial_commit_unreadable_send_envelope_requires_a_check`,
  `partial_commit_evidence_less_send_drop_is_in_flight`,
  `partial_commit_refused_submission_cleanup_failure_is_surfaced`,
  `partial_commit_refused_submission_cleanup_unconfirmed_keeps_refusal`,
  `partial_commit_server_partial_fail_submission_retains_the_email` (no
  destroy request is sent, `Uncertain` `Submitted` primary, committed
  `Created`, no panic),
  `partial_commit_missing_submission_result_records_the_created_email`,
  `partial_commit_draft_send_missing_submission_result_requires_a_check`,
  `partial_commit_reschedule_refused_replacement_reports_cancellation`,
  `partial_commit_chunk_drains_successes_after_the_first_failure`,
  `partial_commit_first_chunk_call_failure_is_a_plain_leaf`,
  `partial_commit_guarded_thread_destroy_later_chunk_refusal_retries`
  (a retryable refusal of chunk 2 after chunk 1 destroyed: `Retry`, committed
  `Deleted` Repeatable),
  `partial_commit_move_thread_remove_leg_failure_keeps_add_leg`; controls kept
  green and asserting check-before-resend with the exact vector
  `Reconcile { PartialCommit, [CheckTarget, DedupeByClientId, ObserveOnly] }`
  (the deleted `incomplete_multi_step_write` produced `PartialCompletionSignal`
  with `[CheckTarget, DedupeByClientId]`; the reason and `ObserveOnly` change,
  the check-before-resend control does not; rewrite whatever exact guidance
  each control asserts today to this vector):
  `acknowledged_send_with_an_email_create_missing_its_id_is_partial`,
  `acknowledged_raw_send_with_an_import_missing_its_id_is_partial`,
  `created_draft_with_missing_email_id_is_partial`,
  `acknowledged_scheduled_send_with_missing_submission_id_is_partial`;
  kept as is: `failed_send_message_submission_best_effort_destroys_created_email`,
  `reschedule_does_not_create_when_cancellation_is_refused`;
  rewrite `a_later_thread_destroy_batch_failure_reports_partial_completion`
  (now: terminal `ProviderContractViolation` for the missing-`newState` arm
  with every destroyed id committed, per 5.4).
  These tests live in `crates/jmap/src/sync/pim.rs`, which only the
  `workspace` and `jmap-sync` sweeps compile, so
  `brokkr test -p bifrost-jmap partial_commit_` reports SKIP on the other
  jmap sweeps; that is expected, and at least one PASS is required.
- IMAP: `brokkr test -p bifrost-imap partial_commit_`:
  `partial_commit_move_fallback_retryable_store_refusal_is_protected`,
  `partial_commit_move_fallback_expunge_missing_capability_keeps_copy`,
  `partial_commit_move_fallback_state_changed_before_send_keeps_copy`,
  `partial_commit_destroy_expunge_failure_marks_deleted` (rewrites
  `expunge_failure_keeps_its_classification_and_adds_the_side_effect_note`),
  `partial_commit_patch_second_store_failure_keeps_add_half` (cache-guarded
  `Flags`: add half `Repeatable`),
  `partial_commit_conditional_patch_add_half_is_blocking`
  (`ConditionalFlags`: add half `Blocking`, protected reconcile, never the
  retry queue),
  `partial_commit_copy_messages_second_folder_failure_keeps_first`,
  `partial_commit_move_thread_attaches_once_across_legs` (remove-leg failure
  after a committed add: one record, no `PartialCommitAlreadyAttached`
  panic),
  `partial_commit_delete_thread_attaches_once`,
  `partial_commit_draft_discard_expunge_failure_marks_deleted`,
  `partial_commit_container_create_refresh_failure_is_protected`,
  `partial_commit_bulk_item_never_carries_siblings`.

The required consumer-decision matrix from the ruling (committed prefix then
local refusal, transport drop, unreadable response, auth or permission failure,
throttled failure with timing and scope; failed and unconfirmed cleanup;
protected JMAP extraction as controls) is covered by 6.1 at the type level and
re-pinned at each producer above.

### 6.4 Green-tree gate

`brokkr check` after every landing. Never `-p`.

## 7. Landings (each one coherent, each `brokkr check` green)

1. **Types and ledger.** Section 3 complete, 3.7 ledger tags, 6.1 and the
   ledger test. No producer attaches a record yet, so every existing recovery
   is unchanged; this is the keep/revert unit for the contract.
2. **Sync consumer.** Section 4 and the 6.2 bulk tests (they attach records
   through `StubAccount` doors, so they need no producer).
3. **DAV.** 5.1 + tests.
4. **Google.** 5.2 + tests.
5. **Graph.** 5.3 + tests.
6. **JMAP.** 5.4 + tests.
7. **IMAP.** 5.5 + tests.

Each landing carries its reference-doc edits (section 8) and the
`notes/bugs-deferred.md` update in the last landing (delete GOOG-027 per that
file's rules; the "no applicable producer" audits go into the reference docs,
not into code comments naming the finding).

## 8. Documentation bricks

- `reference/error-model.md`: new section "Partial commitment" (3.1-3.6, the
  attach-once rule, plan derivation, scalar projection, `leg_recovery`,
  terminal semantics, `Application::from_leaf` as the producer rule, rule 1's
  remediation precedence, directives never lifting `Never`, record-driven
  `DedupeByClientId`, and the `ThrottledMidFlight` reason not surviving
  projection, so consumers read `throttle_observations()` for throttle facts);
  rewrite the `into_builder` paragraph (record attach allowed on rebuilt
  builders; drop "preserves DAV partial-sequence classifications");
  `RecoveryClass` section: `PartialCommit`, `ObserveOnly`; `Fatal` may carry a
  record; telemetry and export tiers; ledger not-preserved list; new scopes;
  the non-idempotent PartialResponse paragraph keeps describing
  `derive_protocol` and stops implying DAV/JMAP use it for sequences; the
  paragraph saying "request and partial-sequence failures must narrow the
  default to reconciliation" is rewritten the same way (sequences now narrow
  through the record, not through a reclassified kind).
- `reference/sync.md`: "Mutation pipeline" (ObserveOnly protection, per-leg
  throttle recording, `partial_commit_observations`), "Ledger envelope" (v7,
  not-preserved list), passthrough wording.
- `reference/types.md`: the new module and exports.
- Per producer: `reference/caldav.md`, `carddav.md`, `google.md`, `graph.md`,
  `jmap.md`, `imap.md` replace every description of the deleted helpers with
  the record each path attaches and its replay judgments; `jmap.md` keeps the
  compensation ordering and evidence gates verbatim, adds cleanup surfacing,
  and records the `serverPartialFail` exclusion from the destroy predicate;
  `imap.md` states the `uid_move_messages` error-shape change
  (`MoveFallbackIncomplete`) and the inner-form split for composed PIM
  operations; `graph.md` records that the send path maps every leg with
  `Send` (the draft leg was `DraftCreate`); `smtp.md`, `google.md`,
  `graph.md`, `jmap.md` gain the no-applicable-producer audit notes from 2.3.

## 9. Stopping rule, exclusions and filed findings

Stops at: the type contract, the bulk consumer, and the producer paths in 2.3.
Out of scope per the ruling: a transaction runner, durable workflow storage,
write reordering, auth/session redesign, the direct PIM set omission finding,
executable or durable continuation. Engine-side processing of direct-write
records (direct writes bypass the engine by design) is not added.

Filed, not built here (lateral findings for `notes/todo.md`):

- IMAP `send_message` / `send_raw_message` / `draft_send`: a failed Sent APPEND
  or draft discard after a committed submission is logged and the call returns
  `Ok`; the consumer cannot learn the Sent copy is missing or the draft
  lingers.
- IMAP `draft_send`: the post-send draft discard failure is likewise only
  logged; the consumer cannot learn the draft lingers.
- IMAP container operations treat a post-write `refresh_folders` failure as
  the operation's failure; reporting success with a stale registry may be the
  better contract (this spec only makes the failure honest).
- Single-request creates whose acknowledged response lacks a usable id
  (IMAP `draft_create` without APPENDUID returns terminal
  `Unsupported(DraftCreate)` for a draft that exists; Google
  `contacts::create` without `resourceName`; Graph `draft_create`; Google and
  Graph filter creates; Google calendar event create): a committed but
  unidentified create is classified terminal, inviting a duplicating manual
  retry. The committed-unidentified shape of 3.4 is the natural carrier; it
  needs its own ruling because these are not multi-step writes.
- Graph `send_message`: a refused send leaves its draft and each automatic
  retry creates another (accepted here as `Repeatable` clutter, 5.3); an
  orphan-draft cleanup like JMAP's would be a new compensation, which 5.7
  rules out of this spec.

Fixed here, not filed: Graph `send_message` mapped its draft-create leg with
`DraftCreate`; the record needs `Send` on every leg, so 5.3 changes it and the
Graph reference records it.

## 10. Ruling requirements to bricks

| GOOG-027 requirement | Where |
|---|---|
| typed representation incl. failed-step recovery and compensation outcome | 3.1-3.5 |
| distinguish known commits, failed-step uncertainty, compensation outcomes | `StepOutcome`, `Application`, `CompensationOutcome` |
| preserve leaf kind, causes, evidence, auth/permission, retry, hint, scope, budgets | 3.2 (leaf untouched), 3.5 rules 1, 3, 5, `leg_recovery`, `throttle_observations` |
| earlier success never acknowledges a later transport failure or turns refusal uncertain | 3.2 `Application::from_leaf`; 3.3 evidence rules; 5.4 refusal arm and `serverPartialFail` exclusion; 6.1 tests |
| never destroy an ambiguously submitted Email | 5.4 `serverPartialFail` arm |
| what recovery applies to | 3.5: `recovery()` operation, `leg_recovery()` leg, roles name changed target |
| no blind replay of a committed prefix; no lost effect on terminal leg | `Blocking`, `ObserveOnly`, barriers keep record; 4.2 observations |
| reconciliation when resumption cannot be established | rule 3 / rule 5 `Never` |
| preserve per-item lanes | 4.1 lane rule; Graph/JMAP drain siblings |
| Google move/PATCH, fields/photo | 5.2 |
| JMAP send/raw/draft evidence, cleanup surfacing, cancel-then-replace | 5.4 |
| DAV move and scheduling-then-local-write | 5.1 |
| IMAP COPY/STORE/EXPUNGE, two-STORE, destroy, multi-folder | 5.5 |
| audit other producers | 2.3 table and audited list |
| consumer reads it | 4.2, 4.3; `recovery()` itself is read by every consumer |
| hermetic tests incl. controls; ablation; full `brokkr check` | 6 |

## 11. Spar record

Codex (read profile), session `01a17269-68a4-7541-a0c2-84510c44e986`, six
exchanges (the sixth a consistency read of this document, whose five findings
are folded in). Opening position: one ordered committed prefix plus one failed step,
merge-on-rebuild, `DedupeByClientId` as the replay guard, retention keyed on
the last error. Conceded after verification against the code:

- one failed step cannot describe independently resolved batch legs (Graph
  batch returns the first failure before the missing sweep; JMAP chunk
  inspection stops at the first failing id) -> sibling legs;
- prepending outer steps widens a compensation's claim -> attach once at the
  public boundary, compensations name the steps they reverse;
- transmission evidence is not application outcome; absent evidence is not
  `Unsent` -> producer-declared `Application`, validated only where evidence
  is explicit;
- the operation table conflates replay safety with duplicate creation ->
  explicit `ObserveOnly`;
- last-error retention loses effects across attempts -> append-only
  observations; requested-state readback does not discharge residual effects;
- missed producers: IMAP container refresh, JMAP two-leg thread ops and
  within-chunk successes, Graph `admit_upload_url`; missing identities need
  scope variants and role-tagged, completeness-flagged targets;
- one winning `RecoveryClass` cannot carry several directives, independent
  prerequisites or per-leg throttle holds -> `OperationPlan` plus per-leg
  `throttle_observations`, with `recovery()` as a safe projection;
- `ObserveFirst` must not project to permanent protection; effectless reads
  must not create observation obligations; leaf `Reconcile` must contribute to
  the plan for totality; a cleared uncertainty must not be observed;
  `InternalFailureAfterSend` must bar reassertion; quota must not collapse to
  rate limit.

Codex conceded `AfterStateRefresh` dominating `AfterAuthRefresh` in the scalar
for built-in request paths (net re-enters the auth lifecycle on every
dispatch), with the narrower wording in 3.5. Final exchange: no further
concrete wrong instruction or dishonest-fill path; codex restated the
mechanism in its own words and it matched.

## 12. Review round 1

Two independent cold reports against `446cd45e`: `partial-commit-spec-r1.md`
(two P1, twelve P2, P3 list, test gaps, one lateral) and
`partial-commit-spec-r2.md` (four P2, one shared with r1). Every finding was
checked against the code before folding. Consolidated, with where each landed:

- C-1 (r1 P1-1). Hand-declared `Application` meets runtime leaves and every
  producer `expect`s `try_build`. Confirmed: `MethodErrorType::ServerPartialFail`
  maps to `Protocol(PartialResponse)`, which on `Send` derives `Reconcile`, and
  the old 3.3 rejected `NotApplied` over a `Reconcile` leaf, so a
  `serverPartialFail` submission would have panicked `send_message`. Folded:
  `Application::from_leaf` and `CompensationOutcome::from_error` (3.1), the
  producer rule (3.2), validation restated in terms of the helper (3.3), every
  "application from the leaf" in section 5 replaced, test
  `partial_commit_application_from_leaf_always_validates`.
- C-2 (r1 lateral, in scope). `submission_create_error` treats
  `serverPartialFail` as a proven refusal and destroys the Email. Folded: 5.4
  `serverPartialFail` arm, 5.7, section 10 row, JMAP test.
- C-3 (r1 P1-2). IMAP `move_thread` and `delete_thread` call the public,
  attaching forms. Confirmed in `pim.rs`. Folded: composition rule (3.2),
  inner forms and owner list (5.5), two IMAP tests. `draft_discard` was found
  during verification to be a further owner of `delete_messages` and is added
  (2.3, 5.5, test).
- C-4 (r1 P2-2). Uncertain `Blocking` legs counted as outstanding blocking, so
  an uncertain send could never be resent even after a negative check, the
  single-leg and recorded shapes disagreed, and 5.3 contradicted the rules.
  Folded as: outstanding blocking is committed `Blocking` steps only; an
  uncertain `Blocking` leg gates (`AfterObservation`) (3.4). Basis: the ruling
  asks uncertain sends to be checked before another send, not never resent;
  the spar already conceded that observe-first must not project to permanent
  protection; observation decides an uncertain leg but cannot undo a committed
  one. Barriers still force `Never`, so JMAP's unreadable or missing
  submission results keep `ObserveOnly`. Adjudicated 2026-10-25: confirmed;
  section 3.4 records the ruling and code basis inline. No owner decision
  remains open on C-4.
- C-5 (r1 P2-3). Known commits with a missing identity were declared
  `Uncertain` to steer the projection. Folded: the committed-incomplete
  `Blocking` gating rule (3.4); JMAP send-without-Email-id, scheduled-send,
  `draft_create`, Graph draft-without-id and import-without-id now record
  committed steps with unknown targets (5.3, 5.4); test
  `partial_commit_unidentified_blocking_create_gates_before_a_barrier`.
- C-6 (r1 P2-4 second half). IMAP `ConditionalFlags` add half is `Blocking`.
  Confirmed in `run_flag_mutation_groups` / `run_patch_mutation_group`.
  Folded: 5.5 kind split, test.
- C-7 (r1 P2-4 first half, r2 4). JMAP guarded thread destroy is
  `Repeatable` at the public boundary. Confirmed: `delete_thread` re-resolves
  and re-reads state each attempt. Folded: 5.4, test.
- C-8 (r1 P2-5). `PartialCommit` reason now requires a committed `Blocking`
  step (3.5 rule 3, rule 5 note), test.
- C-9 (r1 P2-6). Rule 1's precedence over gating gets its reason and the
  guarantee is reworded; rule 2 states that directives never lift `Never`
  (3.5, two tests, section 8).
- C-10 (r1 P2-7). JMAP `unsent` removed with the 3.2 unattempted-ids rule;
  Graph `delete_thread` chunk aggregation (2.3, 5.3, test); a whole-call chunk
  failure is one leg covering the chunk, so a first-chunk failure returns the
  plain leaf (5.4, test).
- C-11 (r1 P2-9). Missing/malformed submission arm records the committed
  Email when its id is readable (5.4, test).
- C-12 (r1 P2-10). Behavior changes stated: JMAP `after_chunk` becomes
  terminal (5.4, rewritten test); JMAP controls' exact vector (6.3); Google
  cloud resulting classes (5.2). `DedupeByClientId` is now record-driven
  (`plan.may_duplicate`, 3.4/3.5), so IMAP moves, destroys and container ops
  gain no dedupe and `MutationCounters::dedupe_by_client_id` is unchanged for
  them; test `partial_commit_dedupe_follows_the_record_not_the_table`.
- C-13 (r1 P2-11). `MoveFallbackIncomplete` delegation: `response_code`,
  `const fn`, `PartialEq`, `Display`, `source()`, reference note (5.5, 8).
- C-14 (r1 P2-12). Google cancel outcomes without an error: never-sent cancel
  records no compensation; unfollowed 3xx is `Failed` from the real response
  (5.2, test).
- C-15 (r2 1). Google `create_sharing_permission` is POST then GET; split at
  the boundary with an effectless GET primary (2.3, 5.2, test).
- C-16 (r2 2). Graph batch progress keeps the new ids of successful moves and
  role-tags them, with the incomplete case gating (5.3, two tests).
- C-17 (r2 3, r1 P3). `StepId` is index-sized (3.1), test past `u16`.
- C-18 (r1 P2-1). The phantom IMAP direct-move owner is removed (5.5).
- C-19 (r1 P3 list). Constructors and derives (3.1); per-effect telemetry
  counts with saturating `u32` (3.6); `PartialCommitAlreadyAttached` wording
  (3.3); owners described by rule and enumerated (3.2); double throttle
  record documented (4.2); `ThrottledMidFlight` latent loss documented (3.5,
  8); Graph raw-import `StepReplay` given (5.3); non-final chunk is
  `Updated(UploadSession)` (5.2, 5.3); `Repeatable`/`Blocking` redefined by
  intended effect, not visibility, with the Graph orphan-draft cost accepted
  and its cleanup filed (3.1, 5.3, 9); JMAP thread-op outer operation stated
  (3.4, 5.4); the second error-model paragraph added to section 8; the Graph
  `DraftCreate` fix moved out of the filed list (9).
- C-20 (r1 test gaps). Covered by the tests named under C-1, C-3, C-6, C-7,
  and the full-vector assertions in 6.2 and 6.3.

Rejected, in whole or in part:

- r1 P2-8, the adoption half. The inconsistency is real and is fixed by
  stating the line (2.3), but adopting IMAP `draft_create`, Google
  `contacts::create`, Graph `draft_create` and the other single-request
  creates is not: they are single effect-bearing requests, outside a ruling
  scoped to multi-step writes, and building them in place would land a
  product decision nobody made. Filed in section 9. Verification found more
  members of the same family than r1 listed (Google and Graph filter
  creates, Google calendar event create).
- r1 P1-1 option (b), non-fatal attachment (drop the record or substitute
  `Uncertain` instead of panicking). With option (a) the declared value is
  derived from the leaf and validation is stated in terms of the same helper,
  so no runtime answer can reach the panic; what remains is a producer bug,
  which the `expect` contract exists to surface loudly in tests. Silently
  dropping a record would hide exactly the defect class this spec closes.
- r1 P1-2, the `draft_send` bullet. `draft_send` calls the private
  `delete_messages` helper and swallows its error; it never receives an
  attached record, so it does not collide with attach-once. Its silent
  discard failure is filed instead (9).
- r1 P2-6, the "limit rule 1" option. Rule 1 keeps `NoPermission` and
  `NeedsPolicyChange` ahead of gating; the reason is stated instead (3.5
  rule 1). A permission refusal carries user remediation that observation
  cannot replace, terminal authorizes nothing, and the gating survives in the
  plan.
- r1 P2-10, the "accept and document the counter change" option for
  `DedupeByClientId`. Rejected in favor of the record-driven rule (C-12),
  since dedupe by client id has no meaning for a move or a destroy.
- r1 P2-11, the precomputed-flag option for the `const fn`. Dropping `const`
  is simpler and costs nothing (no const caller); a cached flag would be a
  second copy of a fact the source already holds.
