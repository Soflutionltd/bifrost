# Spec: bounded desired-state reassertion after a valid read-back mismatch

Technical implementation specification for the `bifrost-sync` finding
"Replay-safe target mismatches need bounded resubmission" (SYNC-007 in
`notes/bugs-sync.md`, as rescoped by the owner on 2026-10-25). Transient: it
lives in `notes/` and dies when the work lands.

## Standing references (read them, do not just note them)

Implementers and reviewers must READ these before touching code. They are the
ground this is built on and judged against.

- `reference/technical-implementation-spec.md` - the contract this document is
  written against.
- `reference/error-model.md` - the cross-cutting `AccountError` contract. This
  work moves no classification. It READS `RecoveryClass` in more places:
  "RecoveryClass", "Central derivation" (in particular the Transport,
  ConcurrencyConflict and Protocol rows: `PartialResponse` on a non-idempotent
  operation derives `Reconcile(PartialCompletionSignal, [CheckTarget,
  DedupeByClientId])`), "Batch and stream outcomes" (the uncertain lane, and
  why `Downgraded` is final), and "Caller mutation preconditions".
- `notes/bugs-sync.md`, entry SYNC-007 - the source item. Its five "The
  specification must" bullets are the acceptance criteria.
- `reference/sync.md` - "Mutation pipeline" (whole section), "Read-back
  guard", "Caller versions on PIM mutations", "Caller-version mutation
  campaigns", and the throttle paragraphs of "Error / Terminated / Warning"
  that describe the campaign wait sites ("A mutation campaign schedules
  throttle cohorts independently").
- `reference/types.md` - "Capabilities" and "Mutation outcomes".
- `reference/imap.md` - the MODSEQ cache paragraphs (opportunistic `STORE
  UNCHANGEDSINCE`), the destroy paragraphs (UID EXPUNGE and the plain EXPUNGE
  fallback), and "Concurrency conflicts and UNCHANGEDSINCE".
- `reference/google.md` - the capabilities list and the flag canonicalization
  and destroy TRASH-fallback paragraphs.
- `reference/graph.md` - the mutation section (etag cache, `If-Match`,
  `permanentDelete`) and the hydration section (REST and EWS arms).
- `reference/jmap.md` - the mutation section (`ifInState` on replacements and
  its one internal re-probe, and the unanswered-id `PartialResponse` lane) and
  "Known limitations".
- `reference/caldav.md` and `reference/carddav.md` - Landing 1 edits both
  crates' capability declarations (and a carddav capability test). Read them
  to confirm nothing in them describes mutation replay or read-back; if
  either does, it is updated in Landing 1.

## The source item, and how this spec reads it

SYNC-007 asks the engine to resubmit eligible unversioned flag writes and
destroys after a valid read-back establishes that their requested target state
is currently unsatisfied, only where reapplying the operation to the same
object is safe. It is explicitly desired-state reassertion: a reasserting
campaign may overwrite a concurrent writer's later change and gains no
lost-update guarantee. That framing was adjudicated by the owner and is not
reopened here.

The premise was checked against the code before this spec was written and
holds:

- `run_bulk_pipeline` (`crates/sync/src/engine/bulk.rs`) reads a cohort back
  once, merges the observations, and marks the cohort `done`;
  `observation_bucket` maps `Mismatched` to `PendingRetry` with no
  resubmission.
- `matches_target_set` (`crates/sync/src/mutation/readback.rs`) compares
  strings exactly, while IMAP's `flags_set` lowercases, so a landed
  `Add(\Seen)` reads back as a mismatch of `\seen`.
- Every provider declares `MutationReplaySafety::None`, `ReplayToken` is
  unused, and `MutationCounters::pending_retry` is a bare count, so consumer
  replay is not an implemented substitute.

Reading decisions this spec makes, each sparred to consensus (see "Review
record"):

1. "Preserve caller-version guards and explicit conflicts" means: a
   caller-precondition refusal (`ConcurrencyConflict` on a target carrying an
   `expected_version`) stays `FailedTerminal` and is never observed or
   replayed, and a caller-guarded target in the read-back lane stays
   unobserved `Unresolved` exactly as today. It does NOT protect
   provider-observed conflicts on unversioned targets (IMAP opportunistic
   `UNCHANGEDSINCE`, Graph `If-Match` from the etag cache, JMAP `ifInState`
   on `FlagOp::Set`). Those derive `Retry(AfterStateRefresh)`; the read-back
   IS the state refresh that advice asks for (it repopulates the IMAP MODSEQ
   cache and the Graph etag cache; JMAP re-probes its state itself), and the
   owner accepted reassertion over concurrent writers. They are reassertable.
2. "Valid observation of the expected projection" is split between engine and
   provider. The engine checks projection and identity. The provider judges
   whether a valid observation already satisfies the target, in the
   provider's own flag vocabulary, because only the provider knows its write
   translation (Gmail label mapping, Graph aliases, server-owned IMAP
   `\Recent`). Producers additionally guarantee that a `Succeeded` FlagsOnly
   hydration is a complete observation.
3. Replay eligibility is a provider declaration, default off. The engine
   cannot know whether a downstream `Account` has stable non-reused object
   identity and same-object reapply safety; the four built-in mail providers
   are analysed below and declare it.
4. Consumer deduplication guidance dominates every other origin signal: any
   reconcile advice naming an action other than `CheckTarget`, carried by a
   failed item, an uncertain item, or a stream termination, makes the id
   protected (observed, never reasserted). A stream termination carrying such
   advice protects EVERY id of its round that it leaves in the read-back
   lane, including ids the round already answered `Uncertain` or `Failed`
   into a reassertable read-back: the termination's guidance covers the whole
   submission, not only the ids it left unanswered. A same-round per-item
   `Retry(SameRequest | AfterAuthRefresh)` answer stays a retry candidate:
   that blind resubmission is today's behaviour, is not a reassertion, and is
   left unchanged (see lateral 6).

## Survey of the ground

### Engine: `crates/sync/src/engine/bulk.rs`

- `run_bulk_pipeline` owns per-campaign state: `outcomes:
  HashMap<ObjectId, MutationBucket>`, `retry_slots` / `retry_owners`
  (`MutationConfig::retry_queue_cap`), `cohorts: Vec<MutationCohort>`,
  `first_error`, `caller_conflict`, `blocked_by_engine`,
  `forwarded_directives`.
- `MutationCohort { identity, key, targets, remaining, submitted, readback,
  done, attempt, retry_at, reconcile_at }`. `attempt` counts submissions;
  `retry_available(attempt, max_retries)` is `attempt <= max_retries`.
- Loop: shutdown check; `aborting = first_error.is_some()`; pause wait unless
  aborting; re-route never-submitted targets; the aborting / engine-blocked
  sweep (never-submitted -> `PendingRetry` / `BlockedByEngine`, submitted
  cohorts -> `readback = true`, and under engine block `PendingRetry` ->
  `BlockedByEngine`); readiness scan (read-back ids are
  the cohort targets that are also in `unresolved_readback_ids(outcomes)`); sleep until earliest
  readiness; admission; post-admission hold and identity re-check;
  `begin_activity`; then either the read-back branch or the submission branch.
- Read-back branch: guarded ids -> `Unresolved` unobserved; the rest go to
  `run_object_readback`; each observation -> `observation_bucket`; a
  stream-level termination -> recorded throttle and `first_error`; the cohort
  is marked `done` unconditionally.
- Submission branch: `cohort.submitted = true`; door chosen by
  `guarded = remaining.any(has expected_version)`; every per-item error is
  recorded as a throttle, and its delay is folded into `reconcile_at` ONLY
  for `Reconcile` and `Retry(AfterStateRefresh)` (an `Uncertain` item
  carrying `Retry(SameRequest | AfterAuthRefresh)` folds nothing, and a
  `ThrottleScope::CurrentOperation` hint never enters the shared bucket, so
  nothing else delays its read-back); per-item outcomes through
  `classify_item_outcome`, with the caller-conflict special case first;
  stream terminations: `Retry` -> `queue_unresolved_for_retry` (every
  disposition), `Reconcile` -> sweep non-final ids to `PendingReadback`,
  `Engine` -> forward + sweep to `BlockedByEngine`, `Terminal` ->
  `first_error`; then `attempt += 1`; omitted ids -> `or_insert`
  (`PendingReadback` or `BlockedByEngine`); retry candidates reserve slots;
  next state is retry (`remaining = next`) or `readback = true`.
- `classify_item_outcome`: `Uncertain` -> `PendingReadback` with no look at
  its error's recovery; `Failed` Retry AfterStateRefresh -> `PendingReadback`;
  unknown disposition -> `PendingReadback`; Reconcile -> `PendingReadback`
  (dedupe counted); Engine -> `BlockedByEngine` + directive; Terminal ->
  `FailedTerminal`; unknown `MutationSuccess` -> `PendingReadback`.
- End-of-campaign sweep: never-submitted -> `PendingRetry`; submitted and not
  done -> `PendingRetry`/`PendingReadback` become `Unresolved`.
- `counters_from_outcomes`: `PendingRetry | PendingReadback | Unresolved` ->
  `pending_retry`.

### Engine: `crates/sync/src/mutation/readback.rs`

- `run_object_readback(account, ids, ReadbackTarget, sink)` returns
  `(HashMap<ObjectId, ObjectReadback>, Option<Error>)`. `ObjectReadback {
  Matched, Mismatched, Unresolved }`.
- Flags: wrong projection -> `matches_target` false -> `Mismatched` (a defect:
  an observation that is not one counts as a mismatch). Move: wrong
  projection -> `Mismatched` (same defect). Destroy: any `Succeeded` ->
  `Mismatched`; `Failed(NotFound)` -> `Matched`; else `Unresolved`.
- The result map is pre-filled with `Unresolved` for every requested id, so
  "not observed" and "observed unresolved" are indistinguishable in it.
  Outcomes are keyed by `success.item.0` only; the hydrated object's own id is
  not checked. A later outcome for the same id silently overwrites an earlier
  one.
- The sink records throttles for every per-item failure, before the id is
  checked against the requested set; an `Engine`-classified per-item failure
  is otherwise ignored.
- Published: `ReadbackOutcome`, `run_readback_guard`,
  `run_move_readback_guard`, `run_destroy_readback_guard` (re-exported from
  `crate::mutation`). Signatures stay.

### Types

- `crates/types/src/mutation.rs`: `FlagOp` (`Add`, `Remove`, `Set`, `Patch`,
  `#[non_exhaustive]`), `FlagOp::validate` (ASCII-case-insensitive identity),
  `HydratedObjectKind::FlagsOnly(HashSet<String>)`.
- `crates/types/src/capabilities.rs`: `MutationCapabilities { concurrency,
  replay_safety }` (plain struct, no `Default`), `MutationReplaySafety {
  ReplayToken, None }`.
- `crates/types/src/account.rs`: `Account` trait; precedent for small
  provider-owned pure hooks with defaults: `throttle_keys`,
  `is_inventory_cursor`.
- `MutationCapabilities { .. }` literals: `crates/types/src/capabilities.rs`
  (if any), `crates/{caldav,carddav}/src/capabilities.rs`,
  `crates/jmap/src/sync/capabilities.rs`,
  `crates/google/src/account/capabilities.rs`,
  `crates/graph/src/account/capabilities.rs`,
  `crates/imap/src/account/capabilities.rs`,
  `crates/imap/src/account/test_support.rs`, and the sync test doubles in
  `crates/sync/tests/{common/mod.rs, readback_guard.rs, filter_passthrough.rs,
  settings_passthrough.rs, convenience_dispatch.rs, attach_schema_recovery.rs}`.
  Run `grep -rn "MutationCapabilities {" crates` at implementation time; the
  list must be exhaustive for the build to pass, and the compiler enforces it.

### Providers: flag observation (FlagsOnly)

- IMAP `crates/imap/src/account/get.rs::fetch_to_hydrated`: `FlagsOnly(
  flags_set(fetch.flags.as_deref().unwrap_or(&[])))` - a FETCH without a
  FLAGS item reads as an empty set. `flags_set`
  (`account/inventory.rs`) lowercases; the set can contain `\recent`, which
  no STORE sets or clears (`types/flag.rs`: read-only). `run_folder_get`
  turns `fetch_to_hydrated == None` (no UID) into `NotFound`; a UID absent
  from the FETCH is `NotFound`; stale UIDVALIDITY is `Request(Malformed)`.
  The FETCH records MODSEQ into the folder cache (`record_modseq`).
- JMAP `crates/jmap/src/sync/hydrate.rs::reconcile_hydration`: `FlagsOnly(
  email.keywords())`; `Email::keywords()` (`email/get.rs`) maps an absent
  `keywords` property to empty.
- Graph REST `crates/graph/src/account/get.rs::hydrated_from_value` ->
  `flags_from_value`: `\seen` from `isRead` (absent -> false), `\flagged` from
  `flag.flagStatus == flagged` (absent or `complete` -> not flagged),
  `category:<name>` per string element of `categories` (absent or non-string
  -> dropped). The REST `$batch` responses are projected by
  `reconcile_hydration_responses`, which harvests each 2xx body's etag BEFORE
  converting it and already turns a 2xx with no body into a per-item
  `Protocol(MissingField)`; the caller writes the harvested etags to the etag
  cache (`etag_index`). Graph EWS public-folder items (`ews_flags`) carry
  only `\seen`, and the EWS parser (`ews/parse.rs`) defaults `is_read` to
  `false` when `IsRead` is missing, so an EWS observation can invent a
  `\seen` absence. Public ids are recognized by
  `super::pim::ews_read_folder(id).is_some()`. The Graph mutation door does
  not route public-folder items to EWS.
- Gmail `crates/google/src/account/inventory.rs::hydrate_one` ->
  `flags::flag_set_indexed`: `\Seen` iff no `UNREAD`; `\Flagged` for
  `STARRED`; `\Draft` for `DRAFT`; `$Important` for `IMPORTANT`;
  `$gmail-label:<id>:<name>` for every other label except INBOX, SENT, TRASH,
  SPAM, CHAT, UNREAD (user labels and `CATEGORY_*`). Gmail omits an empty
  `labelIds`, so absence is a valid empty observation.

### Providers: flag writes

- IMAP `account/mutate.rs`: unversioned targets are partitioned by cached
  MODSEQ; cache hits go out under `UNCHANGEDSINCE` (MODIFIED ->
  `Failed(ConcurrencyConflict)`); `Add` / `Remove` / `Set` are one `UID
  STORE`; a two-sided `Patch` is two STOREs with per-UID accounting. All are
  absolute assertions of flag membership on a UID; reapplying any of them to
  the same UID converges to the same state.
- Gmail `account/flags.rs::translate_flag_op` lowers a `FlagOp` to a
  `LabelPatch { add_label_ids, remove_label_ids, unsupported_flags }`
  computed once against the label vocabulary (`labels_for_flags` on the
  `ScopeCache`, `account/scopes.rs`). `\Seen` is the polarity of `UNREAD`;
  `Set` names every known user label for add or remove and never touches
  `CATEGORY_*` or an omitted `\Draft`; `\Draft` and read-only labels go to
  `unsupported_flags`, which the driver reports as `Downgraded {
  FlagsPartiallyApplied }` (final, never read back). `batchModify` is
  absolute; reapplying it converges.
- Graph `account/mutate.rs::patch_for_flags`: PATCH with `If-Match` from the
  etag cache; `graph_flag_field` accepts `\seen` / `read` and `\flagged` /
  `flagged` / `starred`; `categories_from_flags` takes a token's category
  with a case-SENSITIVE `strip_prefix("category:")`, so `Category:Work` is
  unmodeled and dropped; `Set` writes `isRead`, `flag.flagStatus` and
  `categories` authoritatively; tokens Graph does not model are dropped from
  the body (only an op whose body is empty is refused, by
  `flag_op_is_unexpressible`). PATCH of absolute fields converges.
- JMAP `sync/mutation.rs::apply_flags`: keyword patch (`keywords/<k>`) for
  Add / Remove / Patch, whole-map assignment for `Set` with `ifInState`
  (`uses_if_in_state`); one internal re-probe and retry on `stateMismatch`,
  a second mismatch -> per-item `Failed(ConcurrencyConflict)`. Converges.

### Providers: destroy

- IMAP: `UID STORE +FLAGS.SILENT \Deleted` then `UID EXPUNGE`; without
  UIDPLUS, `expunge_uids_or_fall_back` runs a plain `EXPUNGE` only when `UID
  SEARCH DELETED` is a subset of this batch's UIDs. That check is not atomic
  (documented in `reference/imap.md`); a reassertion runs it once more per
  attempt. Accepted existing limitation, recorded in the IMAP reference by
  this work. Reapplying to a UID that still exists converges; an expunged UID
  is absent and reads back `NotFound`.
- Gmail: `batchDelete`; on a scope refusal, a TRASH move reported as
  `Downgraded { MovedToContainer(TRASH) }` (final). An uncertain TRASH
  fallback re-observed as present is reasserted, re-runs the same fallback,
  and ends `Downgraded`; a reasserted `batchDelete` that now succeeds
  fulfils the original request. Both converge.
- Graph: `POST /messages/{id}/permanentDelete`. Converges.
- JMAP: `Email/set destroy`. Converges.

"Converges" says reapplying is safe. It does not say reassertion is often
reachable: `BulkDestroy` is non-idempotent, so every ambiguous destroy shape
that derives `Protocol(PartialResponse)` carries `DedupeByClientId` and is
`Protected` under reading decision 4 (lateral 3). Which destroy outcomes
actually reach `Reassertable` (derivations per `reference/error-model.md`
"Central derivation"):

| Outcome shape | Derivation | Origin |
|---|---|---|
| Graph unanswered `permanentDelete` `$batch` subresponse (uncertain lane, `batch_response_missing`) | `Reconcile(PartialCompletionSignal, [CheckTarget, DedupeByClientId])` | Protected |
| JMAP id in neither `destroyed` nor `notDestroyed` | same | Protected |
| Transport drop after send, any provider (`InFlight`) | `Reconcile(TransportDropAfterSend, [CheckTarget])` | Reassertable |
| Rate-limit, quota or unavailable answer on an in-flight destroy | `Reconcile(ThrottledMidFlight or TransportDropAfterSend, [CheckTarget])` | Reassertable |
| Internal failure after send | `Reconcile(InternalFailureAfterSend, [CheckTarget])` | Reassertable |
| Omission from a round with no termination | engine-assigned `PendingReadback(Reassertable)` | Reassertable |
| Concurrency conflict | not produced: Graph destroy sends no `If-Match`, so no `AfterStateRefresh` path | n/a |

So for Graph and JMAP the destroy half does real work only on transport
drops, in-flight throttles, internal failures and bare omissions; their
usual ambiguous shape stays observed-only until the owner settles lateral 3.
The `destroy: true` declarations are still correct (they authorize, they do
not promise), and the owner sees this before the destroy half is built.

### Object identity

IMAP ids embed folder, UIDVALIDITY and UID (`encode_object_id`); a UID is
never reused within a UIDVALIDITY, and a changed UIDVALIDITY makes the id
stale (`Request(Malformed)` on read, refused on write). Gmail and JMAP ids
are immutable server ids. Graph ids change on move and are not reused. A
reasserted write therefore never addresses a different object. It may find
the object gone (moved, expunged), which reads back `NotFound`: a destroy
counts that as satisfied, a flag write as unresolved - neither reasserts.

### Tests that pin today's semantics

- `crates/sync/src/engine/bulk.rs` `campaign_tests::
  non_landing_observation_is_pending_not_terminal` calls `observation_bucket`,
  which this work replaces. It is REWRITTEN against the new mapping (same
  claim: a non-landing observation is pending, not terminal), with the
  justification that the function it named no longer exists.
- `crates/sync/src/mutation/readback.rs` has five unit tests on
  `matches_target_set` (`add_target_matches_when_subset_present`,
  `add_target_does_not_match_when_missing`,
  `remove_target_matches_when_absent`, `set_target_requires_exact_match`,
  `patch_requires_both_sides`). They move to `bifrost-types` as
  `flag_target_state` tests (the function moves; the claims are kept and
  extended with casing), with ONE modified claim, justified here as the
  `notes/todo.md` rule requires: `patch_requires_both_sides`'s second case
  is a contradictory `Patch` (`\Seen` in both halves) that today asserts
  not-matched. Under `flag_target_state` that op fails `FlagOp::validate`
  and returns `Indeterminate`, not `Unsatisfied`; the moved test asserts
  `Indeterminate`, because an invalid op is undecidable rather than
  observed-unsatisfied. `destroy_readback_only_accepts_not_found_as_absence`
  stays in `readback.rs` unchanged.
- `crates/sync/tests/readback_guard.rs`: every existing campaign test runs
  against `FlagsAccount`, whose capabilities will declare
  `MutationReassertion::NONE`, so none of them changes behaviour. EVERY
  existing test in the file is the regression net for the unchanged paths
  and must pass unmodified apart from the one-line capability literal; the
  file-wide gate (`brokkr check -p bifrost-sync -- --test readback_guard`)
  runs them all. `cohort_counters_derive_from_per_object_observations`
  feeds EQUAL duplicate observations, which the duplicate rule leaves
  unchanged, so it stays green.
  The published-guard tests (`readback_guard_marks_applied_as_skipped`,
  `readback_guard_handles_remove`, `readback_guard_handles_set`,
  `readback_guard_works_through_arc`) also pass unmodified: their inputs are
  case-consistent and `FlagsAccount` inherits the default judge.
- `bulk.rs` `caller_conflict_is_preserved_instead_of_read_back_as_success`
  and `weaker_success_has_its_own_bucket_and_never_replays` pass unmodified.
- Graph `get.rs` tests calling `flags_from_value` on partial values
  (`flags_from_value(&json!({ "id": "m1" })).is_empty()`) pass unmodified:
  `flags_from_value` stays a total mapper; validation is a separate function.
- Capability tests asserting `replay_safety == None`
  (`imap/src/account/capabilities.rs`, `jmap/src/sync/capabilities.rs`,
  `google/src/account/capabilities.rs`, `graph/src/account/capabilities.rs`,
  `carddav/src/capabilities.rs`) gain an assertion on `reassertion`.

## Target design

### Types (`bifrost-types`)

`crates/types/src/mutation.rs`:

```rust
/// Whether an observed flag state already is the state a flag operation
/// asks for, judged in the observing provider's own flag vocabulary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum FlagTargetState {
    /// The observed state is the requested target state.
    Satisfied,
    /// The observed state is not the requested target state, and applying
    /// the operation would change it.
    Unsatisfied,
    /// The observation cannot decide: the operation names something the
    /// observation cannot see or the write does not model, or the operation
    /// is invalid or unknown.
    Indeterminate,
}

/// The generic judge: ASCII-case-insensitive flag identity, matching
/// `FlagOp::validate` and `canonical_flags_hash`.
#[must_use]
pub fn flag_target_state(observed: &HashSet<String>, op: &FlagOp) -> FlagTargetState
```

Rules of `flag_target_state`: `op.validate()` failing -> `Indeterminate`.
Fold `observed` to a lowercase set. `Add(s)`: every lowercased member of `s`
in observed. `Remove(s)`: no lowercased member of `s` in observed.
`Set(s)`: lowercased-deduplicated `s` equals folded observed (so `{"\Seen",
"\SEEN"}` equals `{"\seen"}`). `Patch { add, remove }`: both halves. `true`
-> `Satisfied`, `false` -> `Unsatisfied`. The match has NO wildcard arm:
`#[non_exhaustive]` does not apply inside `bifrost-types`, the defining
crate, so a `_` arm after the four variants is an `unreachable_patterns`
warning and no test there can build an "unknown" op. A future variant then
fails to compile here and must be classified deliberately. The provider
judges in other crates (IMAP, Graph, Gmail) keep a `_ => Indeterminate` arm.
The `Indeterminate` doc line on `FlagTargetState` ("or the operation is
invalid or unknown") stays: "unknown" is what a downstream judge sees.

`crates/types/src/capabilities.rs`:

```rust
/// Which unversioned bulk mail doors this account declares safe to reapply,
/// after a valid observation shows the target unsatisfied, to the same
/// object. A declaration requires stable, never-reused object identity and a
/// door whose reapplication converges on the requested state without a
/// second, different side effect. It is NOT server deduplication; it
/// authorizes desired-state reassertion only.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct MutationReassertion {
    pub set_flags: bool,
    pub destroy: bool,
}

impl MutationReassertion {
    pub const NONE: Self = Self { set_flags: false, destroy: false };
}

pub struct MutationCapabilities {
    pub concurrency: MutationConcurrency,
    pub replay_safety: MutationReplaySafety,
    pub reassertion: MutationReassertion,
}
```

Moves have no field: the move door is never reassertable (source membership
cannot authorize another COPY; the aggregate-progress contract is the open
"IMAP composite partial completion" item in `notes/todo.md`, a separate
TODO). Re-export `MutationReassertion`, `FlagTargetState` and
`flag_target_state` from `crates/types/src/lib.rs` beside their siblings. The
doc comment on `MutationReplaySafety` is reworded so it no longer says the
engine merely "reads back affected items after retry"; it points at
`MutationReassertion` for the reassertion axis.

`crates/types/src/account.rs`, new provided method on `Account`, placed after
`get_stream`:

```rust
/// Judge one flag observation of `target`, already validated by the caller
/// as a complete `Projection::FlagsOnly` hydration of that id, against
/// `op`. Satisfied means the observed state, read in this provider's flag
/// vocabulary, already is the state this provider's `bulk_set_flags` would
/// establish. Return `Indeterminate` whenever `op` names a token the write
/// does not model or the observation cannot see. Pure: no I/O, no blocking.
///
/// The default is `flag_target_state`. Providers whose write translates
/// flags (aliases, label polarity, server-owned flags) override it.
fn flag_target(
    &self,
    target: &ObjectId,
    observed: &HashSet<String>,
    op: &FlagOp,
) -> crate::FlagTargetState {
    let _ = target;
    crate::mutation::flag_target_state(observed, op)
}
```

The contract that a `Succeeded(FlagsOnly)` hydration is a COMPLETE
observation is added to the `get_stream` doc comment: "A `FlagsOnly` success
carries the object's whole flag state. When the provider cannot establish
the object's whole flag state from the response, the item is a per-item
`Failed(Protocol(MissingField | ParseFailed))`, never an empty set. A
provider whose wire legitimately omits an empty flag collection (Gmail's
`labelIds`) reports that omission as the empty set it means." The wording
deliberately does not say "a response that omits the flag data", which
Gmail's valid empty observation would contradict.

One known exception is recorded rather than fixed: the Graph EWS arm reads a
missing `IsRead` as `false`. Public-folder items never reach a reassertable
outcome (the mutation door does not route them to EWS, and the Graph judge
restricts them to `\seen`), so the exception is harmless for reassertion; it
is written into `reference/graph.md` beside the EWS hydration paragraph so
the trait text and the provider do not silently disagree.

### Providers

Each provider gets a `flag_target` override where stated, a completeness fix
where stated, and its capability declaration. Every override is a free
function placed beside the provider's flag translation (IMAP
`account/inventory.rs` next to `flags_set`, Graph `account/mutate.rs` next to
`graph_flag_field`, Gmail `account/flags.rs` next to `translate_flag_op`)
with a thin trait forwarder, so it is unit-testable without an account.

**IMAP** (`crates/imap`).

- `account/get.rs::fetch_to_hydrated` keeps its signature and every one of
  its existing unit-test callers. The completeness check lives in
  `run_folder_get`: the merged-per-UID map becomes `hydrated_by_uid:
  HashMap<u32, Result<HydratedObject, AccountError>>`; for
  `Projection::FlagsOnly`, a merged `FetchResponse` with `flags == None`
  maps to `Err(e)` without calling `fetch_to_hydrated`, where `e` is
  `super::account_error_with(crate::Error::ProtocolMissing(format!("FETCH
  response lacked FLAGS for UID {uid}")), super::error::ImapErrorContext::
  operation(AccountOperation::Hydrate).with_folder_scope(folder))`. Every
  other merged response goes through `fetch_to_hydrated` as today
  (`None` -> dropped from the map, so the target still reads `NotFound`).
  The verdict loop maps `Some(Ok)` -> `Verdict::Succeeded`, `Some(Err)` ->
  `Verdict::Failed`, absence -> `message_not_found_error`. The new test
  drives this through the crate's scripted-transcript harness
  (`account/scripted_tests.rs`, which already exercises FlagsOnly
  hydration), feeding a `* n FETCH (UID u)` response with no FLAGS.
- `account/inventory.rs` (beside `flags_set`): `pub(crate) fn
  imap_flag_target(observed: &HashSet<String>, op: &FlagOp) ->
  FlagTargetState`: if any token of `op` equals `\recent`
  case-insensitively -> `Indeterminate`; for `Set`, remove `\recent` from the
  folded observed set, then delegate to `flag_target_state`; other variants
  delegate directly. `ImapAccount::flag_target` forwards.
- `account/capabilities.rs`: `reassertion: MutationReassertion { set_flags:
  true, destroy: true }`. `account/test_support.rs` `StubAccount`:
  `MutationReassertion::NONE`.

**JMAP** (`crates/jmap`).

- `email/get.rs`: `pub(crate) fn has_keywords(&self) -> bool {
  self.keywords.is_some() }`.
- `sync/hydrate.rs::reconcile_hydration`: for `Projection::FlagsOnly`, an
  answered email with `!email.has_keywords()` produces
  `ItemOutcome::Failed(BatchFailure::new(item_id,
  super::error::missing_property(id.0.clone(), "keywords", ctx)))`.
- `sync/error.rs`: `pub(crate) fn missing_property(id: String, property:
  &'static str, ctx: JmapErrorContext) -> AccountError`, built like the
  existing `CallNotFound` arm: `Protocol(MissingField)` with
  `Cause::Wire(WireCause::MalformedResponse { protocol: Jmap, detail:
  support_only("Email <id> response omitted <property>") })`.
- No `flag_target` override: keywords are passed through verbatim and the
  generic judge's case folding is exactly RFC 8621 keyword identity.
- `sync/capabilities.rs`: `reassertion: { set_flags: true, destroy: true }`.

**Graph** (`crates/graph`).

- `account/get.rs`: `fn validate_flags_value(value: &Value) -> Result<(),
  (ProtocolErrorKind, &'static str)>`: `isRead` must be a bool
  (`MissingField` if absent, `ParseFailed` if another type); `flag` must be an
  object whose `flagStatus` is a string equal, ASCII-case-insensitively, to
  `notFlagged`, `flagged` or `complete` (`MissingField` / `ParseFailed`);
  `categories` must be an array (`MissingField` if absent) every element of
  which is a string (`ParseFailed` otherwise). The hook lives in
  `reconcile_hydration_responses`, directly after the existing "2xx with no
  body" branch and BEFORE the etag harvest: for `Projection::FlagsOnly`
  only, it validates the body; an `Err` becomes `ItemOutcome::Failed` with
  `protocol_violation(kind, AccountOperation::Hydrate,
  Some(ErrorScope::Message { id }), detail)`, the item `continue`s, and the
  body contributes NO etag to the cache (a body that fails validation is not
  trusted for the etag the next `If-Match` would send). `flags_from_value`
  stays the total mapper it is. The EWS arm is unchanged (its restriction is
  handled by the judge; its `IsRead` default is the recorded exception
  above).
- `account/mutate.rs` (beside `graph_flag_field`): `pub(crate) fn
  graph_flag_target(target: &ObjectId, observed: &HashSet<String>, op:
  &FlagOp) -> FlagTargetState`:
  - Every op token is canonicalized: `graph_flag_field` Read -> `\seen`,
    Flagged -> `\flagged`; a token starting with `category:` stays as is;
    any other token -> return `Indeterminate` (the write does not model it).
    The `category:` prefix test is case-SENSITIVE, exactly like the write's
    `strip_prefix("category:")`, so `Category:Work` is unmodeled and returns
    `Indeterminate` rather than being compared as a category the write never
    sets. Only the category NAME after the prefix is compared
    ASCII-case-insensitively (the generic rule). Pinned by
    `graph_flag_target_refuses_unmodeled_tokens`, which includes a
    `Category:Work` case.
  - If `super::pim::ews_read_folder(target).is_some()` (a public-folder item,
    observed through EWS) and any canonical token is not `\seen` ->
    `Indeterminate`.
  - Otherwise build the canonicalized `FlagOp` of the same variant and
    delegate to `flag_target_state`. For `Set`, the observed set is exactly
    the fields `Set` owns, so plain equality is right.
  - Vocabulary note, pinned by a test: `flagStatus == complete` observes as
    "not `\flagged`", so `Remove(\Flagged)` is `Satisfied` on a completed
    flag. That is the vocabulary meaning and is accepted.
  `GraphAccount::flag_target` forwards.
- `account/capabilities.rs`: `reassertion: { set_flags: true, destroy:
  true }`.

**Gmail** (`crates/google`).

- `account/flags.rs`: `pub(crate) fn gmail_flag_target(snapshot:
  &ScopeSnapshot, observed: &HashSet<String>, op: &FlagOp) ->
  FlagTargetState`:
  0. `op.validate()` failing -> `Indeterminate` (a contradictory `Patch`
     would otherwise translate to overlapping add and remove lists and read
     `Unsatisfied`).
  1. `snapshot.fetched_at.is_none()` -> `Indeterminate` (no vocabulary).
  2. Any op token equal to `\Draft` case-insensitively -> `Indeterminate`,
     before any filtering.
  3. `let patch = translate_flag_op(op, &snapshot.labels)`; non-empty
     `patch.unsupported_flags` -> `Indeterminate`.
  4. Map `observed` back to a label-id set: `UNREAD` unless a `\Seen` token
     is present; `STARRED` for `\Flagged`; `IMPORTANT` for `$Important`;
     `DRAFT` for `\Draft`; `<id>` for `$gmail-label:<id>:<name>` (split on the
     first `:` after the prefix; the name is advisory). Comparisons
     case-insensitive for the system labels, exact for label ids.
  5. `Satisfied` iff every `patch.add_label_ids` id is in the set and no
     `patch.remove_label_ids` id is; else `Unsatisfied`.
  An observed label outside the snapshot vocabulary is never in the remove
  list, matching what the write would do. A snapshot older than the one the
  write path refreshes to can disagree with it; the consequence is a bounded
  futile reassertion, accepted and documented in `reference/google.md`.
  `GoogleAccount::flag_target` (`crates/google/src/account/mod.rs`) forwards
  with `scopes::snapshot(&self.scope_cache)`. Known and accepted cost: the
  forwarder clones the label vocabulary and recomputes `translate_flag_op`
  once per observed id, so a read-back costs O(ids x labels), and a `Set`
  walks every user label per id. Read-back cohorts are bounded by the
  campaign's targets and the work is pure CPU against network round trips,
  so this is not hoisted; `reference/google.md` records it beside the judge
  rules.
- No completeness change: absent `labelIds` is a valid empty observation.
- `account/capabilities.rs`: `reassertion: { set_flags: true, destroy:
  true }`.

**CalDAV / CardDAV**: `MutationReassertion::NONE` (no mail bulk doors).

### Read-back (`crates/sync/src/mutation/readback.rs`)

```rust
pub(crate) enum ObjectReadback { Satisfied, Unsatisfied, Unresolved }

pub(crate) struct ReadbackReport {
    pub(crate) objects: HashMap<ObjectId, ObjectReadback>,
    /// Per-item hydration failures whose recovery is `Engine(_)`.
    pub(crate) engine_failures: Vec<AccountError>,
    /// A stream-level termination, returned as before.
    pub(crate) termination: Option<Error>,
}

pub(crate) async fn run_object_readback(
    account: &dyn Account,
    ids: Vec<ObjectId>,
    target: ReadbackTarget<'_>,
    sink: ReadbackErrorSink<'_>,
) -> ReadbackReport
```

`objects` is still pre-filled with `Unresolved` for every requested id (an id
the stream never answers stays `Unresolved`). Alongside it the function keeps
a local `observed: HashSet<ObjectId>` of ids that have received a real
outcome in this report; the placeholder is NOT an observation. Per item, in
order:

0. Every per-item `Failed` / `Uncertain` goes to the sink (throttles) and,
   if its `recovery()` is `Engine(_)`, to `engine_failures`, BEFORE the id
   check, exactly where the sink is called today. An engine failure for an
   id that was never requested is still reported: engine evidence about the
   account blocks reassertion, the conservative direction.
1. Identity: the envelope id (`item.0`) must be a requested id, else the item
   is ignored (as today). For `Succeeded`, `success.output.id.0` must equal
   `item.0`; a disagreement -> `Unresolved`.
2. Projection: `Flags` requires `HydratedObjectKind::FlagsOnly`, `Move`
   requires `Metadata`; anything else -> `Unresolved`. `Destroy` accepts any
   kind (presence is the observation).
3. Judgement: `Flags` -> `account.flag_target(&id, flags, op)` mapped
   `Satisfied` / `Unsatisfied` / `Indeterminate -> Unresolved`. `Move` ->
   membership contains destination -> `Satisfied`, else `Unsatisfied`.
   `Destroy` -> `Succeeded` is `Unsatisfied`; `Failed(NotFound)` is
   `Satisfied`.
4. Other `Failed` / `Uncertain` -> `Unresolved` (step 0 already reported
   them).
5. Recording: if the id is not in `observed`, insert it and write the
   observation over the placeholder. If it is (a second real outcome for the
   same id): equal observation -> unchanged; different -> `Unresolved`, and
   it stays `Unresolved` for any later outcome too.

`matches_target`, `matches_target_set` and `membership_contains`'s
wrong-projection `false` are deleted. The three published guards keep their
signatures and build `ReadbackOutcome` from the report: `skipped =
Satisfied`, `still_failed = Unsatisfied`, `uncertain = Unresolved`;
`termination` -> `Err`. Their doc comments say the flag guard judges through
`Account::flag_target`. The module doc stops claiming the guard runs "after a
retry" only.

### Engine (`crates/sync/src/engine/bulk.rs`)

State vocabulary:

```rust
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ReadbackOrigin {
    /// A valid unsatisfied observation may authorize reassertion.
    Reassertable,
    /// Observed only: dedupe guidance, engine or terminal recovery on an
    /// uncertain item, or a class this engine does not know.
    Protected,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum MutationBucket {
    Applied,
    Downgraded,
    Skipped,
    FailedTerminal,
    /// A retry candidate (unchanged meaning).
    PendingRetry,
    /// Awaiting observation; the origin decides what an unsatisfied
    /// observation may do.
    PendingReadback(ReadbackOrigin),
    /// Observed unsatisfied and scheduled for reassertion: observation
    /// consumed, attempt budget checked and retry slot reserved. Never read
    /// back again before it is resubmitted.
    ReadyReassert,
    /// Written for every id of a submission round immediately before the
    /// mutation door is called; never survives the round.
    InFlight,
    /// Observed unsatisfied and not (or no longer) eligible. Final.
    Mismatched,
    Unresolved,
    BlockedByEngine,
}
```

`unresolved_readback_ids` collects `PendingRetry | PendingReadback(_)` (not
`ReadyReassert`, `InFlight`, `Mismatched`). `counters_from_outcomes` counts
`PendingRetry | PendingReadback(_) | ReadyReassert | Mismatched | Unresolved`
as `pending_retry`; `InFlight` triggers `debug_assert!(false, ...)` and is
counted pending in release.

Origin classification, new pure helpers:

```rust
fn reconcile_origin(advice: &ReconcileAdvice) -> ReadbackOrigin
// Reassertable iff every action is ReconcileAction::CheckTarget.

fn uncertain_origin(error: &AccountError) -> ReadbackOrigin
// Reconcile(a) -> reconcile_origin(a);
// Retry with SameRequest | AfterAuthRefresh | AfterStateRefresh -> Reassertable;
// Retry with an unknown disposition -> Protected;
// Engine(_) | terminal | unknown RecoveryClass -> Protected.
```

Item delay. In the submission branch, every per-item error whose item lands
in `PendingReadback(_)` folds its own delay into `reconcile_at`:
`Reconcile(advice)` -> `reconcile_delay(advice)` (as today);
`Retry(AfterStateRefresh)` -> `retry_delay(advice)` (as today, with its 1 s
fallback); `Retry` of any other disposition -> `retry_delay(advice)` ONLY
when `advice.retry_hint` is `Some` (new). The hint condition keeps an
unhinted retryable `Uncertain` item, the shape every existing
`readback_guard.rs` campaign test uses (`retryable_transport()` carries no
hint), on today's read-back timing, so those tests stay unmodified; a
provider that stated a delay gets it honoured. This covers an `Uncertain` item carrying
`Retry(SameRequest | AfterAuthRefresh)`, whose hint may be
`ThrottleScope::CurrentOperation` and so never enters the shared throttle
bucket: without the fold its read-back, and the reassertion that follows,
would ignore the provider's operation-local delay. With it, "the reconcile
delay was already honoured before this read-back" (read-back step 4) is true
for every origin. Pinned by test 25.

`classify_item_outcome` changes only these arms: `Uncertain` ->
`PendingReadback(uncertain_origin(&error))`; `Failed` Retry
`AfterStateRefresh` -> `PendingReadback(Reassertable)`; `Failed` Retry
unknown disposition -> `PendingReadback(Protected)`; `Failed` Reconcile ->
`PendingReadback(reconcile_origin(&advice))` (dedupe still counted);
unknown `MutationSuccess` -> `PendingReadback(Protected)`. `Failed`
Engine / Terminal arms and the caller-conflict special case before it are
unchanged: they never enter read-back.

Eligibility, one pure helper used at scheduling and again after admission:

```rust
fn reassertion_declared(op: &BulkPipelineOp, caps: &MutationReassertion) -> bool
// SetFlags -> caps.set_flags; Destroy -> caps.destroy; Move -> false.
```

An id is eligible for reassertion iff: its pre-observation bucket was
`PendingReadback(Reassertable)` or `PendingRetry`; it carries no
`expected_version`; `reassertion_declared(op, &account.capabilities().
mutation.reassertion)` on the account snapshot in hand; `first_error` is
`None`; `blocked_by_engine` is false; `retry_available(cohort.attempt,
max_retries)`; and `retry_owners.contains(id) || retry_slots > 0`.

Submission round (replaces the `or_insert` sweep and narrows the
termination sweeps):

1. After every gate has passed (admission, post-admission hold and identity
   re-checks, `begin_activity`) and immediately before the door call:
   revalidate reassertion. If `remaining` holds `ReadyReassert` ids and any
   of `first_error.is_none()`, `!blocked_by_engine`,
   `reassertion_declared(op, caps of the account just loaded)` fails, those
   ids become `Mismatched` (or `BlockedByEngine` when the failing check is
   `!blocked_by_engine` and `first_error` is `None`, matching the
   top-of-loop sweep below), are removed from `remaining`, consume no attempt,
   and if `remaining` is then empty the cohort is `done`; release admission
   and activity and `continue`. Then write `InFlight` for every id in
   `remaining`, set `cohort.submitted = true`, call the door (door choice
   rule unchanged).
2. Item outcomes overwrite `InFlight` as today.
3. Stream `Terminated`:
   - `Retry` with `SameRequest | AfterAuthRefresh`: retry sweep over ids in
     `InFlight | PendingRetry` (was `None | PendingRetry`).
   - `Retry` with `AfterStateRefresh`: UNGUARDED ids in `InFlight` ->
     `PendingReadback(Reassertable)`; fold `retry_delay(advice)` into
     `reconcile_at`; record the throttle as today. Guarded ids (carrying an
     `expected_version`) in `InFlight | PendingRetry` keep today's retry
     sweep: a resubmission with the frozen caller versions is
     precondition-checked by the server ("Preconditions remain frozen across
     retries"), while a read-back would only file them unobserved
     `Unresolved`. Routing them to read-back would turn a caller-version
     campaign's safe retry into a give-up. Pinned by test 23. The
     stream-advice delay for the retry path is applied as today
     (`stream_termination_advice`).
   - `Retry` with an unknown disposition: unguarded ids in `InFlight` ->
     `PendingReadback(Protected)`; same delay and recording; guarded ids keep
     the retry sweep, as above.
   - `Reconcile(advice)`: let `origin = reconcile_origin(&advice)`. Ids in
     `InFlight` -> `PendingReadback(origin)`. When `origin` is `Protected`,
     ids of this round already in `PendingReadback(Reassertable)` (answered
     `Uncertain` or `Failed` into read-back earlier in the round) are
     downgraded to `PendingReadback(Protected)`: the termination's dedupe
     guidance covers the whole submission (reading decision 4). Ids of this
     round in `PendingRetry` are left to the retry-candidate loop, which
     resubmits them exactly as today (decision 4's note on blind retries);
     an id the loop cannot resubmit is held back by the same budget, slot,
     `first_error` or engine block that make it ineligible for reassertion
     (attempts only grow and slots only shrink), so the protection cannot
     leak through it. Delay, recording, dedupe count and warning as today.
   - `Engine`: forward as today; ids in `InFlight | PendingRetry` ->
     `BlockedByEngine`.
   - `Terminal`: `first_error` as today.
4. Round end, for every id of the round still `InFlight`: if the slot's
   shutdown token is cancelled -> `Unresolved`; else if `blocked_by_engine`
   -> `BlockedByEngine`; else -> `PendingReadback(Reassertable)` (an omission
   is uncertain). `cohort.attempt += 1`. Retry candidates and the next state
   follow the existing code with one change: the retry-candidate loop skips
   any id whose bucket is `BlockedByEngine`, so an item that answered
   retryably and was then swept by an `Engine` termination is not written
   back to `PendingRetry`. Today the top-of-loop sweep repairs that on the
   next iteration, so it is not reachable from a campaign; the skip makes
   the new `InFlight` sweeps not depend on that repair (see the unit test
   `retry_candidates_skip_engine_blocked_ids`).

Read-back branch (after guarded ids are set `Unresolved` as today):

1. Snapshot each observed id's current bucket (its origin) before writing.
2. Run `run_object_readback`. If `engine_failures` is non-empty, set
   `blocked_by_engine` now, and collect the directives that
   `should_forward_engine_recovery` admits (per-campaign dedupe) into a
   local `Vec<(Option<CursorScope>, AccountError)>`. `termination` ->
   recorded throttle and `first_error` as today.
3. Map each observation, iterating the observed ids in the cohort's
   `targets` order (the order the read-back request was built in), never in
   the report's `HashMap` order, so which id wins a scarce retry slot is
   deterministic and test 5 reproducible: `Satisfied` -> `Skipped`;
   `Unresolved` -> `Unresolved`; `Unsatisfied` -> eligible (rule above,
   evaluated now, AFTER step 2 has recorded any termination as
   `first_error` and any engine failure as `blocked_by_engine`, with the
   slot reserved by inserting into `retry_owners` and decrementing
   `retry_slots` on first ownership) ? `ReadyReassert` : `Mismatched`.
   Eligibility is never judged per item as items arrive: an id observed
   unsatisfied before a mid-stream termination must see that termination
   (SYNC-007's "partial observations"; the outcome is pinned by test 24).
   This ordering is the primary guard; the top-of-loop aborting /
   engine-blocked sweep, which finalizes any `ReadyReassert` before another
   round can start, is the backstop, so a per-item implementation would
   still not reassert into an aborting campaign.
4. If any of the cohort's ids is `ReadyReassert`: `cohort.remaining` = those
   ids, `cohort.readback = false`, `cohort.retry_at = now` (holds and
   admission still gate it; the reconcile delay was already honoured before
   this read-back); otherwise `cohort.done = true`.
5. Drop the activity guard and the admission permit, THEN send each
   collected directive as `ReopenRequest::Recovery { scope, error }` on
   `slot.reopen_tx`. The send can wait on a bounded channel served by the
   reopen listener, and a directive's handling can wait for the account's
   activities to drain; sending while this campaign still holds an activity
   could make the two wait on each other. (The existing mutation-item
   forwarding inside the stream loop keeps its current shape; it is not
   touched by this work.)

Aborting / engine-blocked sweep at the top of the loop: for a submitted
cohort, every `ReadyReassert` id becomes `Mismatched` when aborting, or
`BlockedByEngine` when engine-blocked and not aborting (the same mapping
today's sweep gives a scheduled `PendingRetry`: work that was scheduled and
then stopped by an engine directive is reported as engine-blocked, which
SYNC-007's "preserve engine-blocked outcomes" asks for), before
`cohort.readback = true` is set, so a scheduled reassertion is never
observed twice. Pinned by test 22. End-of-campaign sweep: for submitted, not-done cohorts,
`PendingRetry | PendingReadback(_) | InFlight` -> `Unresolved` and
`ReadyReassert` -> `Mismatched`; `Mismatched` and other final buckets are
preserved.

Bound, stated per `Account` call: each cohort makes at most
`mutation_max_retries + 1` mutation-door calls (retries and reassertions
share `cohort.attempt`) and at most that many `get_stream` read-back calls
(a read-back follows a submission round and an observed id leaves the lane).
Provider-internal requests (chunking, etag refresh, JMAP re-probe, IMAP
multi-command sequences) are outside that bound. `retry_queue_cap` bounds the
number of distinct targets that are ever resubmitted, retries and
reassertions together.

Pause, shutdown, throttles and admission need no new mechanism: a
`ReadyReassert` cohort is an ordinary not-done, not-readback cohort and goes
through the same readiness scan, holds (`campaign_keys` over `remaining`),
admission, pause wait and shutdown selects as a retry.

`observation_bucket` is deleted. `MutationCounters` is unchanged in shape;
its `pending_retry` doc comment is reworded to name observed mismatches that
were not reasserted.

## Landings

Two landings, each a coherent change kept or reverted on its gates, with
`brokkr check` green at both boundaries.

### Landing 1: observation validity and the provider judge

Contents: the `bifrost-types` additions (`FlagTargetState`,
`flag_target_state`, `MutationReassertion`, `MutationCapabilities::
reassertion`, `Account::flag_target`, doc changes); every provider change
above (completeness fixes, judges, capability declarations; all test doubles
`NONE`); the read-back rewrite (`ReadbackReport`, identity and projection
checks, judge call, disagreeing duplicates, `engine_failures` collected but
not yet consumed). In `bulk.rs`, only what compiles the new report:
`Satisfied` -> `Skipped`, `Unsatisfied` -> `PendingRetry` (today's
`Mismatched` behaviour), `Unresolved` -> `Unresolved`; `engine_failures`
ignored as today. No campaign behaviour changes except that wrong-projection,
id-disagreeing and incomplete observations are now `Unresolved` and casing
no longer manufactures mismatches.

New tests (each must fail against the pre-landing code where it pins a
defect; see "Bite"):

- `crates/types/src/mutation.rs` tests:
  `flag_target_state_is_ascii_case_insensitive_for_every_op`,
  `flag_target_state_set_equality_folds_case_duplicates`,
  `flag_target_state_refuses_invalid_ops` (an `Add(empty)` and a
  contradictory `Patch` -> `Indeterminate`; no "unknown op" case can be
  built in the defining crate). The five old `readback.rs::tests` comparison
  tests move here, unchanged in claim except the contradictory-`Patch` case
  of `patch_requires_both_sides` (now `Indeterminate`, justified in the
  survey).
- `crates/imap/src/account/scripted_tests.rs`:
  `flags_only_fetch_without_flags_is_a_missing_field_failure` (modelled on
  the existing FlagsOnly `get_stream` transcript test there; asserts a
  `Failed` with `Protocol(MissingField)` for the UID whose FETCH lacked
  FLAGS and a `Succeeded` for a sibling UID whose FETCH carried them).
  `crates/imap/src/account/inventory.rs` tests:
  `imap_flag_target_ignores_recent_for_set`,
  `imap_flag_target_refuses_ops_naming_recent`.
- `crates/jmap/src/sync/hydrate.rs` tests:
  `flags_only_email_without_keywords_is_a_missing_field_failure`.
- `crates/graph/src/account/get.rs` tests:
  `rest_flags_only_requires_every_modeled_field` (each of: absent `isRead`,
  non-bool `isRead`, absent `flag`, unknown `flagStatus`, absent
  `categories`, non-string category element yields `Failed` with the stated
  kind and contributes no etag to the returned list; a complete value
  yields `Succeeded` and its etag), driven through the pure
  `reconcile_hydration_responses`.
  `crates/graph/src/account/mutate.rs` tests:
  `graph_flag_target_canonicalizes_aliases` (`Add({"read"})` on `{\seen}` is
  `Satisfied`), `graph_flag_target_refuses_unmodeled_tokens` (including
  `Category:Work`, whose prefix casing the write does not model),
  `graph_flag_target_restricts_ews_public_items_to_seen`,
  `graph_flag_target_reads_complete_as_unflagged`.
- `crates/google/src/account/flags.rs` tests:
  `gmail_flag_target_compares_labels_by_id_not_name`,
  `gmail_flag_target_set_ignores_categories_and_omitted_draft`,
  `gmail_flag_target_refuses_named_draft_and_unsupported_flags`,
  `gmail_flag_target_without_a_label_snapshot_is_indeterminate`,
  `gmail_flag_target_leaves_an_unknown_observed_label_alone` (a `Set`
  omitting `$gmail-label:Label_9:x` where `Label_9` is not in the snapshot is
  `Satisfied` if everything else matches),
  `gmail_flag_target_reads_seen_as_unread_polarity`.
- `crates/sync/src/mutation/readback.rs` tests (driven through a minimal
  in-file `Account` stub returning scripted outcomes):
  `wrong_projection_is_unresolved_for_flags_and_move`,
  `a_hydrated_id_that_disagrees_with_its_envelope_is_unresolved`,
  `disagreeing_duplicate_observations_are_unresolved` (also feeds a sibling
  id observed exactly once as satisfied and asserts `Satisfied`, so the
  pre-filled placeholder is proven not to count as an observation, and an
  equal duplicate pair that stays `Satisfied`),
  `a_per_item_engine_failure_is_reported_not_absorbed` (also feeds an
  engine failure for an unrequested id and asserts it is reported).
- `crates/sync/tests/readback_guard.rs`:
  `readback_guard_judges_through_the_account` (a NEW test-only
  `JudgingAccount` in that file, separate from `FlagsAccount` so the
  existing double and its literals are untouched: it implements
  `get_stream` for `FlagsOnly` from a fixed table and overrides
  `flag_target` to answer `Satisfied` for an observation the generic judge
  would call unsatisfied; the published guard counts it skipped),
  `readback_guard_folds_flag_case` (observed `{"\seen"}`, `Add({"\Seen"})`
  -> skipped).
- Capability tests in the five provider crates assert the declared
  `reassertion`.

Gates (run in this order; each must pass):

```
brokkr test -p bifrost-types flag_target_state
brokkr test -p bifrost-imap flags_only_fetch_without_flags_is_a_missing_field_failure
brokkr test -p bifrost-imap imap_flag_target
brokkr test -p bifrost-jmap flags_only_email_without_keywords_is_a_missing_field_failure
brokkr test -p bifrost-graph rest_flags_only_requires_every_modeled_field
brokkr test -p bifrost-graph graph_flag_target
brokkr test -p bifrost-google gmail_flag_target
brokkr test -p bifrost-sync wrong_projection_is_unresolved_for_flags_and_move
brokkr test -p bifrost-sync a_hydrated_id_that_disagrees_with_its_envelope_is_unresolved
brokkr test -p bifrost-sync disagreeing_duplicate_observations_are_unresolved
brokkr test -p bifrost-sync a_per_item_engine_failure_is_reported_not_absorbed
brokkr test -p bifrost-sync readback_guard_judges_through_the_account
brokkr test -p bifrost-sync readback_guard_folds_flag_case
brokkr check -p bifrost-sync -- --test readback_guard
brokkr check
```

`brokkr test` filters by test NAME, not by integration-harness file, so a
focused name filter cannot stand for the file. `brokkr check -p
bifrost-sync -- --test readback_guard` forwards `--test readback_guard` to
`cargo test` and runs every test in `crates/sync/tests/readback_guard.rs`
(the unchanged campaign tests listed in the survey included); `-p` keeps the
forwarded target selector out of sweeps whose packages have no such target.
The final bare `brokkr check` is the green-tree gate.

Keep/revert: keep when every gate passes; otherwise revert the landing whole.

### Landing 2: the reassertion state machine

Contents: everything under "Engine" above (`ReadbackOrigin`, the bucket
vocabulary, origin helpers, eligibility, `InFlight` rounds, narrowed
termination sweeps, stream-level state-refresh routing, `ReadyReassert`
scheduling and revalidation, read-back engine-failure forwarding, the sweeps,
counters), plus the documentation listed below.

Test double (a brick of its own, built first in this landing): a new
`ReassertAccount` in `crates/sync/tests/readback_guard.rs`, sharing one
`Arc<ReassertState>` between every instance so a replacement account
aggregates into the same logs. `ReassertState` holds:

- `rounds: Mutex<VecDeque<RoundScript>>`, popped once per mutation-door
  call (ordinary or versioned). `RoundScript { items: HashMap<ObjectId,
  ItemScript>, termination: Option<AccountError>, then: Option<TableEdit>,
  park: Option<Arc<Notify>> }`; `ItemScript = Applied | Uncertain(AccountError)
  | Failed(AccountError) | Omit`. The door collects its targets, logs
  `(door kind, ids with their expected versions, key)`, notifies
  `door_entered`, awaits `park` if present, applies `then` to the
  observation table (models a concurrent writer acting after the write),
  then emits one `Batch` of the scripted items (in target order, skipping
  `Omit`) followed by `Terminated(termination)` if set, else `Done(None)`.
- `observations: Mutex<HashMap<ObjectId, Observation>>` with
  `Observation = Flags(HashSet<String>) | Membership(Vec<MembershipScope>)
  | NotFound | Fail(AccountError)`. `get_stream` logs the requested ids and
  answers per id: `Flags` -> `FlagsOnly`; `Membership` -> `Metadata` with an
  `InventoryEntry` built like the literals in
  `crates/sync/tests/backfill_lane_flow_control.rs`, memberships set;
  `NotFound` -> `Failed` with a `NotFound(Message)` error; `Fail(e)` ->
  `Failed(e)`; an id with NO table entry is omitted from the batch (no
  outcome at all, so the engine sees it `Unresolved`); then
  `Terminated(e)` if `readback_termination: Mutex<Option<AccountError>>`
  holds an error (taken once), else `Done(None)`.
- `keys: HashMap<ObjectId, ThrottleKey>` declared by `throttle_keys` for
  `ThrottleWork::Objects`, so tests choose cohorts and holds.
- Logs: `doors: Mutex<Vec<DoorCall>>`, `reads: Mutex<Vec<Vec<ObjectId>>>`,
  and `door_entered: Notify`.

`ReassertAccount { state, caps }` carries its own `AccountCapabilities`
(the `caps()` helper with `mutation.reassertion` set per test). It overrides
`bulk_set_flags`, `bulk_destroy`, `bulk_move`, and
`bulk_set_flags_if_version` (so a versioned target reaches the script
rather than the inherited refusal). `ReassertFactory` holds a
`Mutex<VecDeque<Arc<ReassertAccount>>>` and pops one per `open`, like
`StubFactory::queue`, so `engine.reattach` installs the next account.

Errors used: `retryable_transport()` (Retry SameRequest); a
`ConcurrencyConflict` error (Retry AfterStateRefresh); a dedupe-bearing
error built as `Protocol(PartialResponse)` with
`.operation(AccountOperation::BulkDestroy)` (non-idempotent, so it derives
`Reconcile([CheckTarget, DedupeByClientId])`); a `CursorInvalid` error with
an account cursor scope (Engine); an `Unsupported` error (terminal); a
rate-limited error stamped with a test `ThrottleBudget` and a
`RetryHint::After` (to record a hold deterministically from inside a
read-back or a round, the way the existing cohort tests do); a rate-limited
error on `AccountOperation::UpdateFlags` with `RetryHint::After` and
`ThrottleScope::CurrentOperation` (idempotent, so `Retry(SameRequest)`, and
never recorded in the shared bucket; built like the one in
`crates/sync/src/engine/reattach_tests.rs`).

Engine unit tests in `bulk.rs` `campaign_tests`:

- `uncertain_origin_protects_dedupe_engine_and_terminal` (table over the
  error kinds above plus a CheckTarget-only reconcile).
- `failed_engine_and_terminal_items_never_enter_read_back` (classify a
  `Failed` Engine and `Failed` Terminal item: `BlockedByEngine` /
  `FailedTerminal`, no `PendingReadback`).
- `non_landing_observation_is_pending_not_terminal` rewritten: `Mismatched`,
  `ReadyReassert` and `Unresolved` all count as `pending_retry`, none as
  `failed_terminal`.

Campaign tests in `readback_guard.rs` (paused time; all with
`ReassertAccount` declaring `{ set_flags: true, destroy: true }` unless the
name says otherwise):

1. `an_eligible_mismatch_is_resubmitted_under_its_frozen_key` - round 1
   `Uncertain(transport)`; observation unsatisfied; round 2 `Applied`.
   Asserts two door calls with equal key parts, one read-back, `applied ==
   1`.
2. `a_case_folded_observation_is_satisfied_not_reasserted` - round 1
   `Uncertain`; observation `{"\seen"}` for `Add({"\Seen"})`. One door call,
   `skipped == 1`.
3. `reassertion_shares_the_cohort_attempt_budget` - `mutation_max_retries =
   1`; round 1 retryable failure, round 2 `Uncertain`, observation
   unsatisfied. Two door calls only, `pending_retry == 1`.
4. `reassertion_with_no_budget_stays_pending` - `mutation_max_retries = 0`;
   round 1 `Uncertain`, observation unsatisfied. One door call.
5. `reassertion_shares_the_retry_queue_cap` - `retry_queue_cap = 1`; two ids
   in one cohort both `Uncertain` then unsatisfied. The second door call
   carries exactly one id; the other is `pending_retry`.
6. `an_undeclared_account_never_reasserts` - `MutationReassertion::NONE`;
   same script as test 1. One door call.
7. `a_guarded_target_is_never_observed_or_reasserted` - through
   `bulk_set_flags_if_version` with one versioned and one unversioned
   target in one cohort, both `Uncertain`; observation table unsatisfied
   for both. Asserts: round 1 is a versioned-door call carrying both ids
   with their versions; exactly one read-back, of the unversioned id only;
   round 2 is an ordinary-door call carrying only the unversioned id
   (scripted `Applied`); counters `applied == 1`, `pending_retry == 1` (the
   guarded id).
8. `dedupe_guidance_is_observed_but_never_reasserted` - three cohorts (three
   distinct throttle declarations): a `Failed` dedupe item; an `Uncertain`
   dedupe item; and a round that answers `c2` `Uncertain(retryable
   transport)` and `c3` `Failed(ConcurrencyConflict)` (both reassertable
   read-back on their own evidence), omits `c1`, then terminates with dedupe
   reconcile advice. All observed unsatisfied, none resubmitted (`c2` and
   `c3` included: the termination protects the answered ids of its round
   too).
9. `an_uncertain_engine_or_terminal_outcome_is_never_reasserted` - two
   cohorts (distinct throttle declarations): round A1 answers `a`
   `Uncertain(CursorInvalid, account cursor scope)` (Engine recovery);
   round B1 answers `b` `Uncertain(Unsupported)` (terminal recovery). Both
   observed unsatisfied (`Flags({})` for `Add({"\Seen"})`). Asserts: exactly
   two door calls (one per cohort), each id read back once, `pending_retry
   == 2`, `blocked_by_engine == 0` (an `Uncertain` item's engine directive
   is not forwarded; out of scope, lateral 1), no error.
10. `moves_never_reassert` - `bulk_move`, round 1 `Uncertain`, observation
    `Membership` holding a folder other than the destination (a VALID
    unsatisfied move observation). One door call, one read-back,
    `pending_retry == 1`.
11. `a_reasserted_omission_is_read_back_again` - round 1 `Uncertain`,
    unsatisfied; round 2 omits the id; second observation unsatisfied;
    round 3 `Applied`. Three door calls, two read-backs.
12. `a_dedupe_termination_after_reassertion_protects_the_round` - round 1
    `Uncertain`, unsatisfied; round 2 omits the id and terminates with
    dedupe reconcile advice; second observation unsatisfied. Exactly two
    door calls, `pending_retry == 1`.
13. `a_held_reassertion_finalizes_on_abort_without_reobservation` - targets
    `a`, `a2` declare budget A, `b` declares budget B (two cohorts, A first).
    Round A1: both `Uncertain`. Round B1: `b` `Failed` rate-limited stamped
    budget B, hint 10 s (B retries at t=10). Read-back A: `a` unsatisfied,
    `a2` `Fail(rate-limited stamped budget A, hint 60 s)` - the sink records
    the 60 s hold on A, so `a` becomes `ReadyReassert` and is held. At t=10
    round B2 terminates with the `Unsupported` error (first error). `b`'s
    table entry is `Flags({})` (unsatisfied): after the abort its
    `InFlight` round-end bucket sends it through the aborting read-back,
    where it is observed once and filed `Mismatched` (ineligible:
    `first_error` is set). Asserts: `a` was read back exactly once; `b` was
    read back exactly once; exactly one door call for cohort A and exactly
    two for cohort B; final counters count `a`, `a2` and `b` in
    `pending_retry` (`a` and `b` as `Mismatched`, `a2` `Unresolved`), error
    is the terminal one.
14. `capability_loss_on_reopen_cancels_a_ready_reassertion` - cohort A only,
    set up as in 13 (60 s hold recorded by the read-back). The factory queue
    holds a second `ReassertAccount` on the same state declaring
    `MutationReassertion::NONE`. At t=5 the test calls
    `engine.reattach(&account)` and awaits it; then advances past 60 s.
    Asserts: one door call for A in total, one read-back of `a`, `a` in
    `pending_retry`, no attempt consumed (the cohort's only door call is
    round A1).
15. `a_read_back_engine_failure_blocks_reassertion_and_forwards_once` -
    three ids in one cohort, all `Uncertain`; observation: id 1 unsatisfied,
    ids 2 and 3 each a `Failed` whose
    error is `SyncState(OperatorOverrideNeeded)` with
    `Cause::State(StateCause::OperatorOverrideNeeded { reason })` (derives
    `Engine(OperatorOverrideRequired)`, whose dispatch auto-pauses the
    account). The test subscribes to `engine.account_control_stream` before
    the campaign and asserts: no second door call; id 1 `pending_retry`;
    exactly one `AccountControl::Pause(PauseReason::OperatorOverrideRequired)`
    received within a window of several seconds of paused time (two
    failures naming the same directive were forwarded once). The campaign
    parks in its
    pause wait like any other campaign on a paused account, so the test
    calls `engine.resume_account` after observing the pause and then awaits
    the campaign's counters.
16. `a_stream_state_refresh_termination_routes_to_read_back` - round 1
    terminates with `ConcurrencyConflict` and no items; one read-back (not a
    blind retry) precedes round 2; observation unsatisfied; round 2
    `Applied`.
17. `destroy_reasserts_only_a_present_object` - two ids `Uncertain` on
    `bulk_destroy`; observation id 1 `Succeeded` (present), id 2 `NotFound`.
    Round 2 carries only id 1; `skipped == 1`, `applied == 1`.
18. `a_concurrent_change_between_rounds_is_judged_on_the_new_state` - round
    1 `Uncertain`, unsatisfied; round 2 `Uncertain`; the script flips the
    table to satisfied before the second read-back: `skipped == 1`, two door
    calls. (The satisfied-at-first-read-back case is test 2 and is not
    repeated here.)
19. `pause_parks_a_ready_reassertion_without_spending_an_attempt` -
    `mutation_max_retries = 1`; cohort A = `a`, `a2` set up as in 13 (round 1
    both `Uncertain`; read-back: `a` unsatisfied, `a2` `Fail(rate-limited,
    budget A, 60 s)`, recording the hold). At t=5 `control.pause().await`; advance to t=90; assert no
    second door call. `control.resume()`; await the campaign. Asserts:
    exactly two door calls for A with equal key parts (the pause consumed no
    attempt, otherwise the budget of one resubmission would be gone),
    `applied == 1` for `a` (round 2 scripted `Applied`).
20. `shutdown_during_a_reassertion_round_leaves_the_target_unresolved` -
    round 1 `Uncertain`, observation unsatisfied, round 2 scripted with
    `park`. The test loops until the `doors` log holds two entries (waking
    on `door_entered` and re-checking the count each time, since `Notify`
    coalesces stored permits), then calls
    `engine.detach(&account)`. Asserts: counters `pending_retry == 1`,
    `error` is `Error::ShuttingDown`, two door calls, one read-back.
21. `repeated_reassertions_consume_the_attempt_budget` -
    `mutation_max_retries = 2`; every round `Uncertain`, every observation
    unsatisfied. Asserts exactly three door calls and three read-backs,
    then `pending_retry == 1`. (This is the test the attempt-consumption
    ablation must turn red: without consumption the campaign keeps going.)
22. `an_engine_block_turns_a_ready_reassertion_into_blocked_by_engine` -
    set up as test 13 up to the held `ReadyReassert` on `a` (cohort A = `a`,
    `a2`; cohort B = `b` retrying at t=10), except that round B2 terminates
    with the `CursorInvalid` error (Engine) instead of `Unsupported`.
    Asserts: exactly one door call for cohort A; `a` read back exactly once;
    `blocked_by_engine == 2` (`a` from the top-of-loop sweep, `b` from the
    `Engine` termination's `InFlight` sweep); `pending_retry == 1` (`a2`
    `Unresolved`); one recovery request forwarded.
23. `a_guarded_stream_state_refresh_termination_still_retries_with_frozen_versions`
    - `mutation_max_retries = 2`; one cohort holding a versioned target `g`
    and an unversioned target `u`, through `bulk_set_flags_if_version`.
    Round 1 terminates with `ConcurrencyConflict` and no items; round 2
    `Applied` for `g`; observation of `u` unsatisfied; round 3 `Applied` for
    `u`. Asserts: round 1 is a versioned-door call carrying both ids with
    their versions; round 2 is a versioned-door call carrying only `g` with
    its unchanged version (the frozen-precondition retry, as today); exactly
    one read-back, of `u` only, after round 2; round 3 is an ordinary-door
    call carrying only `u`; `applied == 2`.
24. `a_read_back_termination_finalizes_observed_mismatches` - round 1
    answers `a` and `b` `Uncertain(retryable transport)`; the read-back
    answers `a` unsatisfied, has no table entry for `b` (omitted), then
    terminates with the `Unsupported` error (`readback_termination`).
    Asserts: exactly one door call; one read-back; `pending_retry == 2` (`a`
    `Mismatched`, `b` `Unresolved`); error is the terminal one.
25. `an_uncertain_operation_local_retry_hint_delays_the_read_back` - round 1
    answers `a` `Uncertain` with the `CurrentOperation` rate-limited error
    carrying a 30 s hint; observation unsatisfied; round 2 `Applied`.
    Advances paused time to t=29 and asserts the `reads` log is empty and
    one door call has happened; advances past t=30 and awaits the campaign.
    Asserts one read-back, two door calls, `applied == 1`.

The retry-candidate change (skip `BlockedByEngine`) is not reachable from a
campaign today: the top-of-loop sweep converts `PendingRetry` to
`BlockedByEngine` on the very next iteration, and an abort cannot intervene
between an engine termination and that sweep (a campaign that is already
aborting submits nothing). It is a structural guarantee for the new
`InFlight` sweeps, so it is pinned at unit level: the loop's candidate
selection is extracted into `fn retry_candidates(remaining: &[ObjectId],
retry_set: &HashSet<ObjectId>, outcomes: &HashMap<ObjectId, MutationBucket>)
-> Vec<ObjectId>` and `bulk.rs` `campaign_tests::
retry_candidates_skip_engine_blocked_ids` asserts an id in `retry_set`
whose bucket is `BlockedByEngine` is not returned (and is not rewritten).

Gates:

```
brokkr test -p bifrost-sync uncertain_origin_protects_dedupe_engine_and_terminal
brokkr test -p bifrost-sync failed_engine_and_terminal_items_never_enter_read_back
brokkr test -p bifrost-sync non_landing_observation_is_pending_not_terminal
brokkr test -p bifrost-sync an_eligible_mismatch_is_resubmitted_under_its_frozen_key
brokkr test -p bifrost-sync a_case_folded_observation_is_satisfied_not_reasserted
brokkr test -p bifrost-sync reassertion_shares_the_cohort_attempt_budget
brokkr test -p bifrost-sync reassertion_with_no_budget_stays_pending
brokkr test -p bifrost-sync reassertion_shares_the_retry_queue_cap
brokkr test -p bifrost-sync an_undeclared_account_never_reasserts
brokkr test -p bifrost-sync a_guarded_target_is_never_observed_or_reasserted
brokkr test -p bifrost-sync dedupe_guidance_is_observed_but_never_reasserted
brokkr test -p bifrost-sync an_uncertain_engine_or_terminal_outcome_is_never_reasserted
brokkr test -p bifrost-sync moves_never_reassert
brokkr test -p bifrost-sync a_reasserted_omission_is_read_back_again
brokkr test -p bifrost-sync a_dedupe_termination_after_reassertion_protects_the_round
brokkr test -p bifrost-sync a_held_reassertion_finalizes_on_abort_without_reobservation
brokkr test -p bifrost-sync capability_loss_on_reopen_cancels_a_ready_reassertion
brokkr test -p bifrost-sync a_read_back_engine_failure_blocks_reassertion_and_forwards_once
brokkr test -p bifrost-sync a_stream_state_refresh_termination_routes_to_read_back
brokkr test -p bifrost-sync destroy_reasserts_only_a_present_object
brokkr test -p bifrost-sync a_concurrent_change_between_rounds_is_judged_on_the_new_state
brokkr test -p bifrost-sync pause_parks_a_ready_reassertion_without_spending_an_attempt
brokkr test -p bifrost-sync shutdown_during_a_reassertion_round_leaves_the_target_unresolved
brokkr test -p bifrost-sync repeated_reassertions_consume_the_attempt_budget
brokkr test -p bifrost-sync retry_candidates_skip_engine_blocked_ids
brokkr test -p bifrost-sync an_engine_block_turns_a_ready_reassertion_into_blocked_by_engine
brokkr test -p bifrost-sync a_guarded_stream_state_refresh_termination_still_retries_with_frozen_versions
brokkr test -p bifrost-sync a_read_back_termination_finalizes_observed_mismatches
brokkr test -p bifrost-sync an_uncertain_operation_local_retry_hint_delays_the_read_back
brokkr check -p bifrost-sync -- --test readback_guard
brokkr check
```

Keep/revert: keep when every gate passes and the bite audit below holds;
otherwise revert the landing whole (landing 1 stays).

### Bite

Per the standing lesson, each new test is ablated: revert the production
change it pins, confirm it fails, restore. Before believing any PASS, check
the run's footer names the intended test (the filter is a substring match).
Minimum ablations:

- Landing 1: remove the new `flags == None` branch in IMAP `run_folder_get`
  (so the merged response goes through `fetch_to_hydrated`, whose
  `unwrap_or(&[])` this design leaves in place; ablating
  `fetch_to_hydrated` would leave the new check active and prove nothing);
  drop the JMAP missing-`keywords` check in `reconcile_hydration`
  (`flags_only_email_without_keywords_is_a_missing_field_failure` red);
  drop the `validate_flags_value` call in Graph
  `reconcile_hydration_responses` (`rest_flags_only_requires_every_modeled_field`
  red); restore the `Mismatched` wrong-projection mapping; drop the
  hydrated-id check; make duplicates overwrite; let the placeholder count as
  an observation (the once-observed sibling in
  `disagreeing_duplicate_observations_are_unresolved` red); remove `\recent`
  filtering; drop Graph alias canonicalization; make the Graph `category:`
  prefix test case-insensitive (`graph_flag_target_refuses_unmodeled_tokens`
  red); drop Gmail id-only label comparison. Each must turn its test red.
- Landing 2: make `uncertain_origin` return `Reassertable` unconditionally
  (8, 9 red); drop the `InFlight` write (11, 12 red); leave `ReadyReassert`
  ids in `PendingRetry` (13 red: two read-backs); skip the post-admission
  revalidation (14 red); stop consuming `cohort.attempt` on reassertion (21
  red); skip the slot reservation (5 red); drop the `BlockedByEngine` skip
  in `retry_candidates` (its unit test red); send read-back directives
  without the per-campaign dedupe (15 red: two pauses); route stream AfterStateRefresh
  back to the retry sweep (16 red: no read-back before round 2); ignore
  `engine_failures` (15 red); limit a `Protected` `Reconcile` termination
  to `InFlight` ids (8 red: `c2` and `c3` resubmitted); map a swept
  `ReadyReassert` to `Mismatched` under engine block (22 red:
  `blocked_by_engine == 1`); route guarded ids of a stream `AfterStateRefresh`
  termination to read-back (23 red: no round-2 versioned call, `g`
  unresolved); stop recording a read-back termination as `first_error` (24
  red: `a` reasserted, a second door call; ablating only the ordering does
  NOT bite, because the top-of-loop sweep backstops it); drop the `Retry`-disposition
  fold into `reconcile_at` for read-back items (25 red: a read-back before
  t=29).

### Documentation, in the landing that changes it

Landing 1:

- `reference/types.md` "Capabilities": add the reassertion declaration and
  what it does and does not mean. "Mutation outcomes": add `FlagTargetState`
  / `Account::flag_target` and the FlagsOnly completeness contract.
- `reference/sync.md` "Read-back guard": rewrite to: projection and identity
  checked by the engine, flag judgement by `Account::flag_target`, destroy
  presence semantics, disagreeing duplicates unresolved, incomplete
  observations unresolved.
- `reference/imap.md`: FlagsOnly without FLAGS is `MissingField`; the
  `\Recent` rule of the judge; the reassertion declaration.
- `reference/jmap.md`: missing `keywords` is `MissingField`; reassertion
  declared. Reword the "Known limitations" bullet that calls the read-back
  guard "the only lost-update protection" so it no longer makes a
  lost-update claim (reassertion supplies none).
- `reference/graph.md`: REST FlagsOnly validation (and that a body failing
  it contributes no etag); judge (aliases, unmodeled tokens, the
  case-sensitive `category:` prefix, EWS restriction, `complete` reads
  unflagged); reassertion declared. The capabilities-list sentence "the
  read-back guard is the lost-update net" is reworded so it makes no
  lost-update claim (same rewording as IMAP and JMAP). The EWS hydration
  paragraph records the FlagsOnly-completeness exception: a missing
  `IsRead` reads as `false`, harmless for reassertion because public-folder
  items never reach a reassertable outcome.
- `reference/google.md` capabilities list: reassertion declared; judge rules,
  the accepted snapshot-staleness futility, and the accepted O(ids x labels)
  judge cost. The destroy TRASH-fallback paragraph's claim that
  "`bifrost-sync` read-back-verifies the `Downgraded` lane ... the guard
  files it `still_failed`" is already stale (`Downgraded` is final and never
  read back) and is rewritten in Landing 1 to say a `Downgraded {
  MovedToContainer(TRASH) }` is final and never read back. Landing 2 adds
  that an UNCERTAIN fallback re-observed as present is reasserted and ends
  `Downgraded`.
- (Written in Landing 2, when reassertion exists; listed here beside the
  judges it annotates.) Accepted futility from server-maintained flags,
  recorded beside each judge in `reference/google.md`, `reference/imap.md` and
  `reference/jmap.md`: a `Set` (or an op touching a server-maintained flag)
  can be unsatisfiable when the server re-applies or refuses state on its
  own (Gmail's classifier re-adding `IMPORTANT`, servers adding attachment
  keywords, an IMAP server without `\*` in PERMANENTFLAGS accepting but not
  persisting a new keyword). After an uncertain write such a target reads
  back `Unsatisfied` every time and spends the cohort's attempt budget on
  writes that cannot stick; that is bounded by the attempt budget and
  accepted, like the Gmail snapshot staleness.
- `reference/caldav.md` and `reference/carddav.md`: `MutationReassertion::NONE`
  noted wherever the capability declaration is described; nothing else if
  neither describes mutation replay.

Landing 2:

- `reference/sync.md` "Mutation pipeline": rewrite step 4 (observation
  consumption, `ReadyReassert`, eligibility, shared budget and slots, the
  per-Account-call bound), the `Retry` / `Reconcile` bucket bullets (origins;
  "A mismatch does not trigger automatic replay" is replaced by the
  reassertion rule and its exclusions), the `ItemOutcome::Uncertain`
  sentence (observed, then reassertable unless protected), the stream-level
  `AfterStateRefresh` routing (unguarded ids to read-back, guarded ids keep
  the frozen-version retry), the termination-wide dedupe protection, the
  item-delay fold, the destroy reachability table from the survey (which
  destroy shapes reach `Reassertable`), and the move paragraph (moves never
  reassert).
  "Caller-version mutation campaigns": state that caller-guarded targets are
  never reasserted and define "explicit conflict" as a caller-precondition
  refusal. File map line for `readback.rs` ("Projection::FlagsOnly read-back
  guard") updated to name the report and the judge.
- `reference/imap.md`: the sentence "The engine's read-back-after-retry path
  is the lost-update safety net" is reworded (read-back plus desired-state
  reassertion; no lost-update guarantee); the destroy paragraph records that
  a reasserted destroy re-runs the non-atomic plain-EXPUNGE fallback once per
  attempt.
- `reference/google.md` destroy TRASH-fallback paragraph: the reassertion
  clause (see Landing 1).
- `crates/sync/src/mutation/mod.rs` module doc and `bulk.rs` doc comments on
  `bulk_set_flags` / `run_bulk_pipeline` / `classify_item_outcome` /
  `MutationBucket` updated to the new lanes.
- `notes/bugs-sync.md`: SYNC-007 is removed when landing 2 is kept.

## Stopping rule

In scope: the types additions, the four mail providers' FlagsOnly
completeness, judges and declarations, the read-back module, and the
campaign state machine in `bulk.rs`, with their docs and tests.

Out of scope, by name:

- Moves. Excluded until the "IMAP composite partial completion" TODO
  (`notes/todo.md`) supplies an aggregate-progress contract.
- Server deduplication (`MutationReplaySafety::ReplayToken`): unchanged.
- Engine-directive forwarding for `Uncertain` items whose error carries an
  `Engine` recovery. Today such an item is read back and its directive is
  not forwarded; this spec only makes it protected. Filed as a lateral
  (below), not built here.
- Graph mixed flag operations that name both modeled and unmodeled tokens:
  the write silently drops the unmodeled tokens and reports `Applied`. The
  judge returns `Indeterminate` for such ops, so reassertion never acts on
  them, but the misreported `Applied` is a separate defect. Lateral.
- Exposing per-object outcomes on `MutationCounters`: not required by the
  item once the engine reasserts itself.
- `Projection::Metadata` completeness (move read-back): moves never
  reassert, and their observation fix here is limited to wrong projection.

The teardown stops at `run_bulk_pipeline`'s signature: entry points,
`MutationCounters`' fields, `IdempotencyVendor`, scheduler and throttle
plumbing are untouched.

## Laterals noticed while surveying (to file, not to build here)

1. An `Uncertain` mutation item whose error recovery is `Engine(_)` never
   forwards its directive (`classify_item_outcome`); the account keeps
   receiving work an engine action was asked for.
2. Graph `patch_for_flags` drops unmodeled tokens from mixed operations and
   the item reports `Applied` instead of `Downgraded {
   FlagsPartiallyApplied }` (`flag_op_is_unexpressible` only refuses an
   empty body).
3. `BulkDestroy` is non-idempotent, so a destroy `PartialResponse` carries
   `DedupeByClientId`, which is meaningless for a destroy and makes such
   targets permanently non-reassertable under this spec's rule. For Graph
   and JMAP that is the USUAL ambiguous destroy shape (see the destroy
   reachability table in the survey), so the destroy half of this work is
   narrow for them until this is settled. An operation-aware derivation is
   an owner-level error-model question.
4. The two existing engine-directive send sites inside the submission
   stream loop (per-item `Engine` and stream `Engine`) `.await` on the
   bounded `reopen_tx` (capacity 16, `engine/attach.rs`) while holding the
   campaign's `activity` and admission permit. This spec moves the NEW
   read-back forwarding after those are released because a directive whose
   handling waits for the account's activities to drain could wait on the
   sender; if that hazard is real for the new path it is real for the
   existing two. Not changed here.
5. Legacy `imap1:` object ids never correlate: `decode_object_id` accepts
   the legacy prefix, but every outcome envelope (hydration and mutation) is
   re-encoded `imap2:`, so a campaign or read-back keyed by a legacy id never
   matches its own outcomes (reads back `Unresolved`; mutation outcomes land
   under a different key from the cohort's targets). Predates this spec;
   the new hydrated-id check does not make it worse.
6. A stream termination carrying dedupe guidance does not stop a same-round
   per-item `Retry(SameRequest | AfterAuthRefresh)` answer from being
   resubmitted blindly by the retry-candidate loop. That is today's
   behaviour and this spec keeps it (reading decision 4); whether a
   whole-submission dedupe signal should also suppress same-round blind
   retries is an owner question.

## Review record

Sparred with a fresh codex session (`review -p read --provider codex`,
session `01a17180-b1d6-7cd1-925c-baec560f97a5`) over three exchanges before
any of this was written. Positions taken in and how they moved:

- Agreed from the start: build it; provider-owned judge over an engine judge
  on a richer observation type (an engine judge would duplicate each
  provider's write translation); unversioned provider-observed conflicts are
  reassertable and "explicit conflicts" means caller-precondition refusals;
  moving stream-level `AfterStateRefresh` to read-back is sound, destroy
  included.
- Conceded after verification (each checked against the code):
  Graph needs a full judge, not only an EWS exception (`graph_flag_field`
  aliases); Graph REST completeness must cover `flag.flagStatus` and every
  `categories` element, not only `isRead`; dedupe protection must dominate
  the uncertain origin; the outcome map cannot prove a fresh omission
  (`or_insert`), hence `InFlight` rounds; a re-read of non-eligible
  mismatches had to be prevented, hence the final `Mismatched` bucket; a
  scheduled-but-unsent reassertion must not be re-observed on abort, hence
  `ReadyReassert`; `InFlight` is written only after every gate; eligibility
  is checked at scheduling and revalidated after admission on the loaded
  account; an `Uncertain` item with `Engine` or terminal recovery is
  protected; a read-back per-item `Engine` failure blocks reassertion and
  forwards its directive; Gmail's judge must use the label snapshot and the
  translator, not vocabulary alone (an observed label outside the snapshot
  would otherwise cause futile reassertion); the bound is per `Account`
  call, not per wire request; the IMAP plain-EXPUNGE fallback limitation is
  recorded rather than claimed safe.
- Final correction: `Failed` items with `Engine` or terminal recovery keep
  their existing branches (never read back); only `Uncertain` ones become
  protected. With that, the partner stated it found no remaining
  correctness defect in the mechanism.
- Fourth exchange, an audit of this written document against the code:
  the retry-candidate loop could rewrite an engine-blocked id (fixed with
  the `retry_candidates` skip, pinned at unit level because no campaign can
  reach it today); read-back directive forwarding moved after the activity
  and admission are released; `brokkr test <NAME>` filters names, not
  harness files, so the file-wide regression gate is `brokkr check --
  --test readback_guard`; the attempt ablation needed its own test (21);
  the test double gained a versioned-door override, membership
  observations, parking and per-round table edits, and a queue factory for
  `reattach`; the IMAP completeness check moved into `run_folder_get` so
  `fetch_to_hydrated` and its unit tests are untouched; the Gmail type is
  `GoogleAccount`; the Gmail judge validates the op first; test 15 now
  supplies two failures naming one directive.
- Fifth exchange, confirming the revisions: the partner withdrew its own
  campaign counterexample for the retry-candidate case (unreachable, unit
  pin appropriate), and asked for `-p bifrost-sync` on the forwarded
  `--test readback_guard` gate and a count-checked wait in test 20 instead
  of two `Notify` waits. Both applied. It reported no other remaining
  correctness or buildability defect.
- Cold reviews r1 and r2 (`notes/sync-readback-reassertion-spec-r1.md`,
  `-r2.md`), consolidated and each finding checked against the code before
  folding. Overlaps: r1 #1 and r2 #1 (stream dedupe termination left
  answered ids reassertable) are one finding; r1 #5 and r2 #3 (the IMAP
  completeness ablation targeted untouched code) are one finding. Folded,
  all severities:
  - Dedupe termination protects the whole round's read-back ids (decision
    4, termination step 3, test 8 extended, ablation). While folding it,
    the retry-candidate interaction was checked: same-round per-item blind
    retries stay as today and cannot leak protection, because the budget,
    slot, error and engine-block conditions that stop a retry also make the
    id ineligible; recorded as lateral 6.
  - Read-back delay for `Uncertain` items with retry advice (r2 #2): the
    code folds only `Reconcile` and `AfterStateRefresh` delays, and a
    `CurrentOperation` hint never enters the shared bucket. Fold added,
    limited to hinted advice so existing campaign tests keep their timing
    (test 25).
  - IMAP ablation retargeted to `run_folder_get`; JMAP and Graph
    completeness ablations added (r1 #5, r2 #3).
  - `flag_target_state` has no wildcard arm in the defining crate; test
    renamed (r1 #2).
  - Destroy reachability table; the destroy half is narrow for Graph and
    JMAP (r1 #3, confirmed against `reference/graph.md` and
    `reference/jmap.md` and the derivation in `bifrost-types`).
  - Guarded ids of a stream `AfterStateRefresh` termination keep the
    frozen-version retry (r1 #4, confirmed: today's
    `queue_unresolved_for_retry` resubmits them; test 23).
  - Swept `ReadyReassert` under engine block -> `BlockedByEngine` (r1 #6,
    confirmed against today's sweep of `PendingRetry`; test 22).
  - Deterministic slot order in the read-back (r1 #7).
  - Explicit `observed` set; engine failures for unrequested ids reported
    (r1 #8, confirmed: today's map is pre-filled).
  - Partial observation through a read-back termination pinned (r1 #9,
    test 24), with the rejected part noted below.
  - Five moved comparison tests and the changed contradictory-`Patch` claim
    (r1 #10).
  - Test bricks: test 9 specified, test 18's duplicate half dropped, test
    13's `b` observation and the double's missing-id behaviour defined,
    `JudgingAccount` named (r1 #11).
  - `get_stream` completeness wording no longer contradicts Gmail; Graph
    EWS `IsRead` default recorded as an exception (r1 #12, confirmed in
    `ews/parse.rs`).
  - Case-sensitive Graph `category:` prefix (r1 #13, confirmed in
    `categories_from_flags`).
  - Stale `reference/graph.md` and `reference/google.md` claims added to
    the documentation list (r1 #14, both confirmed).
  - CalDAV and CardDAV references added (r1 #15).
  - Small inaccuracies: judge placement, the full `readback_guard.rs`
    regression set, the Graph hook location and etag handling, the Gmail
    judge cost accepted and documented (r1 #16).
  - r1's laterals: the existing directive sends under an activity and the
    legacy `imap1:` ids filed as laterals 4 and 5; server-maintained-flag
    futility folded into the documentation list as accepted futility.

  Rejected, with reasons:
  - r1 #9's claim that a per-item eligibility implementation "would
    reassert into an aborting campaign, and every listed test would stay
    green". It would not: the top-of-loop aborting sweep finalizes any
    `ReadyReassert` (to `Mismatched`) before another round can start, so
    the ordering rule has a backstop. The requested test is kept for the
    outcome it pins, but the requested ordering ablation cannot bite; its
    ablation instead stops recording the read-back termination.
  - r1 #12's alternative of making the Graph EWS arm fail on a missing
    `IsRead` is declined in favour of the other option r1 offered
    (recording the exception): public-folder items never reach a
    reassertable outcome, and the provider fix would be out-of-scope work.
  - r1 #16's alternative of hoisting the Gmail translation out of the
    per-id path is declined in favour of documenting the cost, the other
    option r1 offered.
