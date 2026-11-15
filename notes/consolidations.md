# Consolidations - design hunt, November 2026

One decision answered in more than one place. The unit of an entry is the
question, not the site: every site that answers it belongs to the one entry,
along with where its single owner should live. Each entry is the hunters'
account, unverified.

1. An entry is removed entirely when completely resolved. No historical record
   stays here.
2. Stable IDs never change and are never reused; removal leaves a gap.
3. An entry adjudicated against, verified incorrect, or whose outcome is that no
   action is taken owes comments at the code sites it names - and, where the
   claim touches a documented contract, the relevant `reference/` or `docs/`
   page - before the entry is removed, so the finding is not hunted again.
4. Once all findings are resolved, the file gets deleted.
5. Finding IDs are never written into the code or other documents. They are
   stable only until this document is drained; the next hunt writes new ones,
   and they are never deduplicated through git history. Carry the context
   inline instead.

Each entry names the hunt scope(s) that reported it. Where hunters disagreed,
both readings are recorded. The raw reports are in the commit that precedes
this file's.

## CON-002 - What class of failure is this? (kind and cause restated at every builder)

**Reported by:** types (LATENT), graph.

The graph hunter found holes in the pairwise check: `access_kind_matches_cause`
matches `MailboxUnavailable { .. }` against `MailboxUnavailable { .. }`, so a
`Transient` kind paired with a `Permanent` cause builds cleanly, and
`server_kind_matches_cause` accepts any status when one side is `None`. Graph's
`classify`, `classify_by_status`, `soap_fault_to_account_error` and
`autodiscover_error` write about forty literal pairs such as
`(AccountErrorKind::NotFound(r), Cause::Request(RequestCause::NotFound { what: r,
.. }))`, and Graph adds a third parallel copy - its classifiers' throttle bool
(BUG-083). Its owner is the same: `AccountErrorBuilder::from_cause(cause)`
deriving the kind, with a separate constructor taking the protocol sub-kind for
the `Wire` case.

Every producer answers it twice, `AccountErrorBuilder::new(kind, cause)`, and
`recovery::kind_matches_cause` rejects disagreement at runtime. The pairs are
nearly 1:1: `AuthErrorKind`/`AuthCause`, `AccessErrorKind`/`AccessCause`,
`SyncStateErrorKind`/`StateCause`, `ServerErrorKind`/`ServerCause`,
`InternalErrorKind`/`InternalCause::kind`, and `TransportErrorKind`/
`TransportKind`, which are the same three variants under two names. Exceptions
are small: `Request(Malformed)` accepts `Malformed | InvalidArgument`;
`Protocol(_)` accepts any `Wire(_)` (the protocol kind is not in the cause at
all); `ServerErrorKind::Error`'s status may be absent on either side.

Builder-time invariants are further runtime checks of shapes a type could make
unconstructable: `throttle_scope`/`throttle_budget` only on `RateLimited |
QuotaExhausted` (they belong inside those `ServerCause` variants);
`CursorInvalid` requires an `ErrorScope::Cursor` (it belongs in
`StateCause::CursorInvalid { scope }`). The sibling scope-requiring kinds
disagree on the rule: `CursorInvalid` without a cursor scope is rejected, while
`ScopeRevoked` and `ScopeCapabilityLost` without one silently become
`RestartAccount` (`derive_sync_state`).

Related: HTTP/SMTP status lives in three places - `ServerErrorKind::Error {
status }`, `ServerCause::Error { status }`, and `DiagnosticInfo::status`.
`recovery::derive_server` reads the kind's, falling back to the cause chain; the
diagnostic copy is never consulted; `kind_matches_cause` accepts `None` on
either side. One status, owned by the cause.

Owner: make the cause primary. `AccountErrorBuilder::new(cause)` with
`Cause::kind()` derived; move `ProtocolErrorKind` into `WireCause` (or a
`Cause::Protocol { kind, wire }`); carry throttle and scope data in the causes
that need them. Deletes `kind_matches_cause`, `KindCauseMismatch`,
`ThrottleScopeNotApplicable`, `ThrottleBudgetNotApplicable`,
`CursorInvalidWithoutScope` and most of roughly 350 `try_build().expect(..)`
sites. The hunter calls this a big rewrite of every error translation boundary
and the right one. Changing the builder signature is a shape change; removing
`TransportKind` or the build-error variants removes published items and needs
the owner.

## CON-003 - Which protocol is this? (six vocabularies, mapped outside their crate)

**Reported by:** types (LATENT, one LIVE wildcard).

Vocabularies: `cursor::ProtocolKind` (6), `error::Protocol` (9 + `Unknown`,
adds Smtp, Lmtp, Ews), `error::Provider` (vendors), `mutation::ProtocolSalt` (6,
string payloads always empty), `events::PushSource`, dav-core's `DavProtocol`.
`FilterCondition::ProviderExpression { provider: Provider }` uses the vendor
enum where every other "which protocol minted this" field uses `ProtocolKind`.

Mappings, all outside the defining crate and therefore forced into wildcards by
`#[non_exhaustive]`:

- `sync/src/recovery.rs::checkpoint_protocol`: `ProtocolKind -> Protocol`, `_ =>
  Protocol::Unknown`, derived from a checkpoint although the engine holds the
  account and `Account::protocol()`.
- `sync/src/mutation/idempotency.rs::default_salt_factory`: `ProtocolKind ->
  ProtocolSalt`, `_ => ProtocolSalt::Imap` - exactly the catch-all the
  `ProtocolSalt::CalDav` doc comment says stops being harmless "the moment the
  salt acquires meaning".
- dav-core `error.rs`: `DavProtocol -> Protocol`.
- `recovery::derive_server` decides "is this an SMTP reply" from
  `Cause::Wire(WireCause::Smtp(_))` (`smtp_reply`), not from the error's own
  `protocol` field, so one error answers "which protocol" with two fields.

`Account::protocol()` defaults to `Protocol::Unknown` "for compatibility", yet
every implementation overrides it; `unsupported_inventory_stream` keeps a
`_scope` parameter for compatibility and defaults to `Protocol::Unknown` while
its `_with_protocol` sibling is what callers want. Providers stamp
`.provider(..)` and `.protocol(..)` at each builder call (Gmail about 15 sites,
Graph about 14) although the account knows both once.

Owner: `bifrost-types`. `From<ProtocolKind> for Protocol` (exhaustive inside
the crate), make `Account::protocol()` required, and stamp protocol/provider at
the account boundary (an error context the account owns) rather than per
builder. Collapsing the enums removes published items and needs the owner.

The sync-lanes hunter adds a third source of truth: the consumer passes
`protocol: ProtocolKind` into every `bulk_*` campaign entry, used only to pick a
`ProtocolSalt` that "never travels on the wire" (`default_salt_factory`, and
`IdempotencyVendor::salt_for: Box<dyn Fn(ProtocolKind) -> ProtocolSalt>`
configures something with no reader); `recovery::batch_boundary_violation`
reads the protocol off the offending checkpoint's `OpaqueChangeState::protocol`
tag; and the two enums each have their own codec (`envelope.rs::encode_protocol`
and `ledger_envelope.rs::encode_protocol`). Nothing checks the consumer's
`ProtocolKind` against the account. Proposal: derive the salt from the account
and drop the parameter (owner ruling: published).

## CON-004 - Does this account run push, and on which lanes?

**Reported by:** types (LATENT).

Answered by `AccountCapabilities::push: PushCapability` and by
`Account::push_lanes()` / `PushLaneKind`. The engine re-combines them at three
sites (`sync/src/engine/attach.rs` seeding push health,
`sync/src/push/lanes.rs::PushLanes::run`, `sync/src/engine/reattach.rs`
carrying subscriptions), each spelling `if caps.push == None { [] } else {
push_lanes() }`. The three non-`None` variants carry nothing the engine reads:
in-process vs out-of-process is already per lane in `PushLaneKind`, and
`OutOfProcessPubsub` vs `WebhookOrEwsStream` encode provider identity.

Owner: derive the capability from the lanes (`None` iff no lanes; in-process iff
any `Connection` lane), or one `effective_push_lanes(&dyn Account)` in
`bifrost-types`.

## CON-006 - Capability flag vs the method's own refusal

**Reported by:** types (LATENT; `PimMethodSupport` was ruled 2026-10-03 to stay, and nothing here proposes removing it).

Each provider sets a flag in `capabilities.rs` and independently refuses in the
method body. Related sites the ruling does not cover:

- `MutationConcurrency::ExpectedVersion`'s doors are read by no consumer. The
  engine sends `MailMutationTarget` versions without consulting them
  (`sync/src/engine/bulk.rs`), and the trait's default `bulk_*_if_version`
  refuses without consulting them either.
- The `send_as` rule (whether a rejected `Some(send_as)` gets
  `Unsupported(Send)` or `Request(Malformed)`, keyed on the flag) is restated by
  Graph and JMAP and held by `capability_contract_tests.rs`, which caught both
  answering wrong once.
- IMAP's `build_capabilities` hand-merges composed DAV sub-accounts' doors and
  hard-codes `event_rsvp: false` rather than reading CalDAV's.
- `Account` methods, `PimMethodSupport` fields and `AccountOperation` variants
  mirror each other by hand (the `PimMethodSupport` doc concedes it).

The imap-account hunter reports IMAP's pairs: `build_capabilities` decides
`send_message`/`draft_send` from `submission_configured`, `filters_*` from
`has_sieve`, `draft_create` from "Drafts exists and UIDPLUS"; the methods decide
again from `account.submission.is_some()`, `config.sieve.is_some()`,
`role_folder(Drafts)` plus "APPENDUID came back". `capability_contract_tests.rs`
is the pairwise-agreement test holding them in step, and its own header records
that it found three flags not gating their methods. Container create / rename /
move / delete read no flag at all.

The jmap-account hunter reports JMAP's version: `capabilities::build` derives
`PimMethodSupport`'s sixty bools from a `PimSupport` struct of eight bools, while
the doors gate independently on `Option<MailAccount>` handles and on
`max_delayed_send != 0` (`cancel_scheduled_send`, `reschedule_send` restate
`scheduled_send: submission && max_delayed_send > 0`). The factory keeps the two
derived from one fact today (`resolve_optional_families`), and
`capability_contract_tests.rs` checks the agreement only in the false direction,
with a fixture building `PimSupport` and the handles by hand, so a factory change
letting them drift would not be caught. The copies already disagree on shared
containers (BUG-066). Its shape: a slot-owned capability set from which both the
flags and the door gates read (`slot.require(Family::Submission)?`).

The graph hunter reports Graph's `capabilities::build_capabilities` hand-writing
about sixty `PimMethodSupport` bools mirroring which methods refuse, with
`capability_contract_tests.rs` catching drift only in the false direction (its
module doc says so); it caught a live violation the day it was written. It argues
the 2026-10-03 reasoning covers the trait shape, not the mirror, and that a
per-method `fn supports(&self, op: AccountOperation) -> bool` derived from the
same match the refusing default dispatches through would make flag and refusal
one decision - worth re-opening at the types level, since every account crate
carries the same mirror.

The google hunter reports Google's `capabilities.rs::PimMethodSupport` table and
its `capability_contract_tests.rs` header calling itself "a hand-maintained
mirror", checked one way only, and proposes the `Account` default for each optional
method consult `capabilities().pim_methods.x` and refuse before calling an
implementation hook, so a `false` flag IS the refusal rather than a promise of one;
or derive the table from which hooks a crate implements.

The net-dav hunter reports both DAV crates carry a `capability_contract_tests.rs`
of about 585 lines, mirrored between them, to pair hand-set `PimMethodSupport` /
`ExpectedVersion` bools with the method bodies' refusals; the DAV kernel of STR-152
would at least let one dialect-generic impl and one test serve both.

Owner, without removing anything: a `bifrost-types` helper turning a flag into
the canonical refusal (`caps.pim_methods.gate(AccountOperation::X)?`,
`caps.mutation.concurrency.gate_version(door, version)?`,
`classify_send_as(&caps, ..)`), and a declarative macro listing each optional
method once and generating the flag field, the operation variant and the
default refusal.

## CON-007 - Does this send have at least one recipient?

**Reported by:** types.

Sites: `mime::render::send_request_to_rfc5322`, Google `pim.rs` (its own
`require_recipient` branch), IMAP `pim.rs` draft send, JMAP scheduled send.
Owner: a `SendRequest` / `DraftPatch` validator in `compose.rs`.

## CON-008 - Is this stored cursor an inventory resume position?

**Reported by:** types.

`Account::inventory_resume_stream` and `Account::is_inventory_cursor` "must
accept EXACTLY" the same cursors; only Graph overrides, and only one of them.
Owner: one method returning a classification (`Option<InventoryResume>`), which
removes the second answer.

## CON-009 - Which throttle budget does this work consume, and which does this error stamp?

**Reported by:** types.

An implementation must declare in `throttle_keys` every budget its errors
stamp. Google mints the same budget at `client.rs` (quota project) and again in
`error.rs::translate_net_error` from the `LocalHold` key string; Graph mints in
`client.rs`. Owner: a per-crate budget constructor used by both the declaration
and the stamp, so the pairing is structural.

## CON-010 - How are a composite account's capabilities merged?

**Reported by:** types.

Only IMAP composes sub-accounts and only IMAP knows how to merge their
`AccountCapabilities`. `account_compose.rs` claims to be the reusable
composition layer ("tomorrow JMAP / Graph") but holds no capability merge. See
STR-016 for where the composition layer should live.

The imap-account hunter describes the merge: `build_capabilities` copies each
contact and calendar `PimMethodSupport` field and each
`MutationConcurrency::ExpectedVersion` field from the sub-accounts by hand, and
`mod.rs` forwards each contact/calendar method by hand; `event_rsvp` concurrency
is hard-coded `false` and `event_rsvp_if_version` is not forwarded (harmless while
CalDAV refuses tokens, but nothing ties the two). Every new PIM field needs a
remembered edit. Its proposal: group `PimMethodSupport` and the expected-version
table by lane (`contacts: ContactMethods`, `calendars: CalendarMethods`), so
composition is one assignment per lane and new fields arrive automatically.

## CON-011 - Are `ObjectType::Event` and `ObjectType::CalendarEvent` the same domain?

**Reported by:** types (LATENT disagreement), graph.

The graph hunter adds: Graph mints `Event` (`cursor::scope_for_kind`); CalDAV and
IMAP-composed calendars mint `CalendarEvent`; Google matches both; and Graph
carries local alias machinery for the duplication (`inventory::event_aliases`, the
`scope_for_kind_collapses_calendar_event_onto_event` test, and the renewal
session's "ambiguous case - `Event` and `CalendarEvent` scopes over one calendar"
branch). Nothing in types says how the two differ: document a real difference or
collapse them (a published removal, so the owner rules).

Graph treats them as aliases (`graph/src/account/inventory.rs::event_aliases`,
`cursor.rs`, `push/webhook.rs`); `account_compose::route_typed_scope` routes
only `CalendarEvent` to the calendar sub-account; the engine keys them as
distinct `CursorScope`s. Owner: `bifrost-types`, one variant or a stated alias
rule.

## CON-012 - Given a stream-terminating `AccountError`, what does this lane do?

**Reported by:** sync-engine (sites already disagree).

Every long-lived lane matches `plan_recovery` itself and decides what to record
in the throttle bucket, how long to sleep (and the fallback), whether and how to
forward (stamped or not), and whether to broadcast `Terminated`. Sites:
`multiplexer/mod.rs::handle_drive_outcome`, `push/reconciler.rs`,
`push/lanes.rs`, `push/mod.rs::route_watch_event`, the lifecycle reader
(`lifecycle_termination` plus `lifecycle_retry_wait`),
`engine/reattach.rs::handle_account_error`, `re_establish_scope_with_backoff`,
`bulk.rs` (item and stream), deferred inventory (through `ctx.recover`).

Already disagree: retry fallback with no hint is 1s in the poll loop and
campaigns, `Duration::ZERO` in push lanes, a 30s floor in lifecycle. Terminal:
the poll loop parks and logs, the sink broadcasts `Terminated`,
`handle_account_error` only logs ("broadcast already carried the terminating
event", true only for some producers). Throttle recording: the push lanes record
holds; a retryable sink termination records a hold and then reports only a
prose warning.

Owner: one `StreamFailure` dispatcher in `recovery` taking the error, scope,
incarnation stamp and lane kind, performing the shared side effects once
(record holds, telemetry), and returning a typed action (`Retry { after }`,
`Forward(ReopenRequest)`, `Terminal`). Lanes keep only genuinely lane-specific
policy, such as the lifecycle reader's await-reopen rule.

The sync-lanes hunter reported the same question independently, adding: the
reconciler's comment says it does this "exactly as the poll loop's
`handle_drive_outcome` does", yet on an account-wide `Engine(_)` verdict the
reconciler holds the remainder for 30s while the poll loop returns with no exit
and carries on at cadence; the wait-for-the-restart bound is written three
times (`LIFECYCLE_REOPEN_WAIT` in the multiplexer, a literal
`Duration::from_secs(30)` in the reconciler hold, a literal in `drive_lane`'s
restart wait); the `retry_delay` / `reconcile_delay` floors are literal per site.
Its proposed shape: `recovery.rs` returns `Disposition { record:
ThrottleRecord, wait: Wait, directive: Option<ReopenRequest>, park: bool,
announce: Option<Warning> }` from one function taking a `DriverKind` where the
answer legitimately differs, with the 30s bound one named constant.

## CON-013 - Is this error a throttle, and for how long? (four-plus recorders, two hold stores)

**Reported by:** sync-engine (sites already disagree).

Recorders: `recovery::record_throttle_parts` (budgets win, mailbox gets a 1s
fallback, otherwise a hint is required); `recovery::record_repair_throttle`
(also fires on `RetryReason::RateLimited` or `ReconcileReason::ThrottledMidFlight`
with no scope or hint, returns a local deadline with a 1s fallback);
`bulk.rs::record_error_throttle` (walks `error.throttle_observations()` per leg,
while `classify_item_outcome` separately records the primary leg - "max
deadlines make that duplicate harmless"); the push lanes and lifecycle reader
through `record_throttle`. So whether a partial commit's sibling-leg throttle is
honoured depends on which lane saw the error: campaigns record every leg; the
poll loop, reconciler, repair and push lanes only the primary.

Owner: `bifrost-types`, `AccountError::throttle_holds() -> Vec<(HoldTarget,
RetryHint)>` covering every leg, budgets and scope lifting, reducing the engine
to `ThrottleBucket::observe(account, &error) -> Option<Instant>`. Also make
`ThrottleBucket` own its lock instead of free functions over
`&std::sync::Mutex<ThrottleBucket>`.

Cross-crate question to settle: `bifrost-net` has its own hold store
(`SharedHold` and `hold_gate`, string-keyed, `HoldKind::{RateLimited,
QuotaExhausted}`, used by Google), and the engine has `ThrottleBucket` (keyed by
`ThrottleKey`, used through stamped budgets by Graph and IMAP). Both answer "is
this provider budget held right now" with different keys, clocks and lifetimes,
and an error from a net local hold is recorded into the engine bucket a second
time. Decide which store owns provider holds and feed the other from it.
Related: CON-009 (budget declared vs stamped).

## CON-014 - May the engine start wire work for this account now? (the drive-start gate)

**Reported by:** sync-engine.

The protocol: (1) boundary is `Run`; (2) activity registered; (3) account-wide
holds slept out first; (4) the current account's declared keys checked; (5)
admission taken; (6) keys re-checked against the freshly loaded incarnation
after admission; (7) the fence re-checked; (8) on a hold, the permit released.
Written by hand in the poll loop (and again inside the `with_drive` closure),
the push reconciler, the backfill orchestrator, the partition runner through
`LaneGate`, the deferred inventory worker, the campaign scheduler, and
`RepairPacing::wait` with `restart_account`. Each copy carries its own
admission-refusal backoff (cadence, 1s, 100ms, or a `record_attempt` ramp).
`orphan_teardown.rs`, `repair_debt` and the passthrough cluster skip some or all
of it (BUG-009, BUG-014). The "drive-start granularity" guarantee is held
together by about seven parallel copies.

Owner: one `DriveStart` primitive on the slot,
`admit_drive(work: ThrottleWork, kind: WorkKind) -> Result<Admitted { permit,
activity, incarnation }, Held { scope_local: bool, until }>`, owning steps 1-8
and re-checking after every internal wait. Lanes keep only their reaction to
`Held { scope_local: true }` (requeue, defer or park). Orphan teardown and
repair go through it too.

## CON-016 - Which cursor scopes does a membership map to, and back?

**Reported by:** sync-engine, types.

- `engine/attach.rs::scope_covers_membership`: the predicate from a
  `CursorScope` to the memberships it covers (feeds push-hint routing).
- `multiplexer/mod.rs::membership_scopes` and `folder_cursor_shapes`: generate
  cursor scopes from a membership for lifecycle create and delete.
- `recovery.rs::resolve_throttle_key`: turns `ErrorScope::Cursor(Folder(id))`
  into `ThrottleKey::Mailbox(MailboxId(folder.0))`.

All three treat `FolderId` and `MailboxId` as the same string via `.0` ("Graph
maps folders 1:1 to mailboxes"). They agree today; nothing ties them. Adding
`MembershipScope::Label` handling or a new `CursorScope` shape means editing two
inverse functions in two modules. The `Type(_)` arm also "covers every
membership", including memberships of an unrelated object type. Owner:
`bifrost-types`, `CursorScope::covers(&MembershipScope)` plus a shape-driven
`scopes_for(membership)` tested as its inverse, with Folder vs Mailbox an
explicit conversion. The type-level overload behind it is STR-001.

The sync-lanes hunter counted five sites and two answers on folder-vs-mailbox
identity, adding: `inventory_walk::removals` projects a folder scope's removal
only to `MembershipScope::Folder`, never `Mailbox`, and an unknown `CursorScope`
is an `Internal(AccountContract)` error; the `mutation/readback.rs` move check
(`entry.memberships.contains(destination)`) is strict enum equality, so a
`Folder("x")` destination never matches an observed `Mailbox("x")` - the
opposite of the string-identity rule in `scope_covers_membership`; and
`cursor::ScopeShapes::record` / `folder_cursor_shapes` decide which shapes
exist. Every site carries a `_ =>` arm because both enums are
`#[non_exhaustive]` across the crate boundary, so a new variant becomes false,
ignored or an error depending on the site. Its proposed owner: one
`cursor/topology.rs` owning `covers(scope, membership)`,
`removal_projection(scope)`, `scopes_for(shapes, membership)` and
`membership_eq(a, b)`; or `bifrost-types` next to the enums, where the matches
can be exhaustive, if the equivalence is a protocol fact rather than engine
policy.

## CON-017 - Establishing a cursor is one operation written twice, and attach bypasses the writer

**Reported by:** sync-engine.

`attach.rs::establish_one` and `reattach.rs::run_establish` (whose doc says it
"Mirrors `SyncEngine::establish_one`") each read the stored cursor; decide
readable vs unreadable with the same `Ok(Some(c)) if validate_envelope().is_ok()
/ Ok(Some(_)) | Err(SchemaIncompatible)` pattern and react differently by
design; ask `is_inventory_cursor(&c) || inventory_resume_stream(c).is_some()`;
handle `Ready` vs `EstablishViaInventory`; and fail an unknown variant with
`EstablishCursorFailed("unknown CursorEstablishment variant")`. They already
diverge on fresh-envelope classification (BUG-006). The attach path also writes
the store directly (`persist_cursor` as `get_ledger` then `apply_transition`, and
`clear_account_checkpoints`), while recovery goes through the writer: two
implementations of "persist an established cursor preserving the ledger" and of
"account schema clear".

Owner: spawn the ack writer first in attach (it depends on no other worker), and
run attach's establishment through the same `Establishment` routine as recovery
and reattach, with an origin mode (`Attach`, `Recover`, `Reattach { attempt }`)
deciding the real differences (persist-ready or stage; claim provisional rows;
defer to fusion or run it inline). That makes "one writer per account owns every
durable mutation" literally true; today the attach-time clear and persist are
the exceptions.

## CON-018 - Which priority maps to which scheduler lane, and when does starvation fire?

**Reported by:** sync-engine.

`scheduler/mod.rs::lane_index`, `AdmissionState::visit_order` and
`record_grant` re-implement the work-item path's `Scheduler::lane` and
`try_pull` starvation rule ("exactly as `Scheduler::try_pull` does", "matching
`Scheduler::lane`"). Two queue types back them: `LaneQueue` with a shed policy
and `VecDeque<AdmissionRequest>` with a refuse policy. They agree by inspection.
Owner: one `PriorityLanes<T>` with a pluggable overflow policy owning the floor
rule once; the published, engine-unused work-item path stays as a thin user of
it.

## CON-019 - Who publishes on the change stream, and through which door?

**Reported by:** sync-engine.

`changes_tx: broadcast::Sender<MultiplexerEvent>` is threaded alongside
`delivery: Arc<ChangeDelivery>`, which wraps the same sender, into
`AccountSlot`, `MultiplexerHandle`, `SlotContext`, `RecoveryContext` and
`BackfillWiring`. Each call site picks a door: `publish_backfill`,
`publish_acknowledgeable`, `delivery.sender().send`, or `changes_tx.send`
(reattach, bulk, push). The "numbered receiver took delivery" invariant is
enforced only for callers that pick the right one, and hand-built
`MultiplexerEvent { scope: CursorScope::Account, checkpoint: None, publication:
None, .. }` literals appear in at least six places. Owner: the raw sender private
to `ChangeDelivery`, which gains `publish_warning(EventScope, Warning)` and
`publish_terminated(EventScope, AccountError)`; no context carries `changes_tx`.

The sync-lanes hunter reported it from the producer side: a backfill page is
published by threading `changes_tx`, `delivery: &ChangeDelivery`,
`control: &SyncControl` (wrapping the publication ledger) and
`coverage: &Arc<PendingCoverage>` (the same ledger again) into `run_partition`,
and fusion does the same with `Option<Arc<ChangeDelivery>>`, `control` and
`coverage`. Each call site then picks raw sender vs gate ("events nobody
acknowledges send on the raw sender" is a convention the runner's barrier page
and both front ends' terminations follow by hand), control vs coverage
("Through control when there is one"), and fenced registration or not
(STR-039). Its proposal: one per-account `Publisher` owning the broadcast
sender privately with the ledger and the delivery gate, offering
`publish_page(page, claim, fence) -> PageOutcome`, `publish_marker(..)`,
`publish_unacked(event)` and `announce(attention)`; producers get a `Publisher`
and nothing else, so "did this reach a numbered receiver, and if not, retire and
record" is decided once inside `publish_page`.

## CON-020 - What does a push teardown refusal mean?

**Reported by:** sync-engine.

`SyncEngine::unsubscribe_push`, `subscribe_push`'s refused-registration cleanup,
`reattach.rs::unwind_replacement_subscriptions`, `retire_stranded` and
`orphan_teardown.rs` each call `push_unsubscribe`, decide whether a refusal
becomes an orphan, and log. Only the unwind path records throttle holds
(through `RepairPacing`); only the retrier applies a timeout. Owner: a
`SubscriptionTeardown` service on the registry, with "delete, settle or orphan,
record holds" as one operation.

## CON-022 - Ledger mutation: apply then persist, or persist then commit?

**Reported by:** sync-engine.

Inside `ack_writer` the in-memory ledger is mutated two ways. Mutated in place
and kept even if `put_ledger` fails: `RecordDebt`, `RecordBarrier`, `ResetScope`,
`ResetAccountForSchemaRecovery`, the discard passes. Cloned, persisted, and
replaced only on success: `CrossWaivedBarriers`, `OperatorDecision`,
`ApplyRepair`. The split is defensible (debt is monotone and must not be lost;
policy must not take effect unpersisted), but each arm decides it independently,
and `persist_ack_request` mutates in place before an `apply_transition` that can
fail. Owner: make the rule a type - `ledger.accrue(..)` for monotone, sticky
debt; `ledger.stage(..) -> Staged` for policy and proof, committed by the
successful write. The read-side divergence this causes is BUG-008.

## CON-024 - What does an inventory front end do with one page?

**Reported by:** sync-lanes (sites already disagree).

The question: project entries to `Created` ids, project removals, decide
whether the page crosses a waived barrier, decide whether it stops the walk,
record barriers, warn, judge the fence, and publish. Answered by
`BackfillRunner::run_partition` (twice internally: the barrier-page and normal
branches each build the `kept -> Change::ObjectChange { Created }` chain and
the synthetic `Batch`), `InventoryFusion::run_stream` plus
`forward_inventory_batch`, and `repair.rs::publish_recovered` (down-converts
recovered entries to `Created` and publishes per scope). The crossed-or-stop
pattern (`let crossed = if coverage.has_barrier() {
cross_waived_barriers(..).await? } else { false }; if !crossed && let
StopAtBarrier = walk.inspect(..)`) is written four times: runner batch, runner
`Done`, fusion batch, fusion `Done`.

Disagreements: degraded coverage is announced by fusion and never by backfill
(BUG-017); fusion judges the fence before forwarding a checkpoint-less barrier
page (`forward_inventory_batch` checks `fence_moved` when nothing registers)
while the runner's barrier page goes out on the raw sender unfenced; the
boundary-violation outcome is `Err(Error::Account)` in the runner but
`Ok(FusionOutcome::Terminated)` in fusion; the barrier warning text is
duplicated verbatim between `warn_barrier` and `warn_degraded`; a failed barrier
write is logged in fusion and silent in the runner. `inventory_walk.rs` records
an explicit trigger for unifying the minting sites, "a THIRD inventory front end
appears"; the hunter says repair is that third front end in everything but
checkpoint minting, so the trigger has fired.

Owner: `InventoryWalk` grows `async fn judge(&mut self, coverage, writer) ->
PageVerdict { Advance, CrossedWaived, Stop { resume_from } }`, owning the
crossing, the stop, barrier recording and the degraded flag, plus one
`PagePublisher` (CON-019) owning entry and removal projection, the
fence-before-forward rule and the warnings. Only the checkpoint source (minted
positional vs account-supplied) stays per front end.

## CON-026 - Backfill completion state is tracked four times in memory

**Reported by:** sync-lanes.

`BackfillScopeWalk::stopped` (one bool); `ScopeWalkDriver::{exhausted,
completed}`, which holds a `BackfillScopeWalk` and also re-derives stop;
`BackfillScan::settled` (per incarnation, with a watermark); and
`BackfillRegistry` (engine-wide, keyed by `(AccountId, CursorScope)` without
incarnation, marked from the same answer, published through
`SyncEngine::backfill_registry`). The registry and the scan agree because the
orchestrator writes both; with no incarnation, a re-established scope reads the
previous incarnation's state until overwritten. Owner: the registry becomes a
read-only projection of `BackfillScan` (STR-044), keyed by incarnation.

## CON-028 - IMAP folder role from attributes and names: two tables, already different

**Reported by:** imap-protocol (live, minor).

- `types/mailbox.rs::MailboxInfo::special_use()`: attributes, then the name
  table {"all", "all mail", "sent", "sent items", "sent messages", "drafts",
  "draft", "trash", "deleted", "deleted items", "deleted messages", "spam",
  "junk", "junk e-mail", "bulk mail", "archive", "archives", "flagged",
  "starred", "important", "inbox"}; leaf by `rsplit` of a lowercased name.
  Production-dead (only decoder tests call it).
- `account/pim.rs::folder_role` + `folder_role_from_attributes`: attributes (no
  `\All`, `\Flagged`, `\Important`; `\Inbox` via `Custom`), then the name table
  {"inbox", "sent", "sent mail", "sent messages", "draft", "drafts", "archive",
  "archives", "trash", "deleted", "deleted messages", "junk", "spam", "junk
  mail"}; leaf via `folder_registry::leaf_name`.

"Sent Items", "Deleted Items" (Exchange) and "Junk E-mail" are recognised by the
dead copy and not the live one; "Sent Mail" (Gmail) the reverse. The method on
the protocol type is the one the next consumer will reach for. Owner: one
classifier in `types/mailbox.rs` (attributes authoritative, one name table,
namespace-aware per BUG-028), with `pim` mapping `SpecialUse -> FolderRole`.

## CON-029 - What does an IMAP MYRIGHTS set permit?

**Reported by:** imap-protocol (latent).

`types/acl.rs`: `can_read` = l+r, `can_insert` = i, `can_delete` = t+e; legacy
`c` / `d` parsed but never interpreted. `account/pim.rs::rights_from_myrights`:
`may_remove_items` = t alone (no e), `may_rename` / `may_delete` = x or legacy
d, `may_create_child` = k or legacy c. `folder_registry::listing_selectable`:
`can_read`. `can_insert` / `can_delete` are production-dead and disagree with
the live projection (t+e vs t); RFC 4314 section 2.1.1's legacy expansion lives
only, and partially, in pim. ACL rights also travel as strings (`MyRights {
rights: String }`, `AclEntry { rights: String }`, `ListRights { required:
String, optional: Vec<String> }`, `Command::SetAcl { rights: String }`,
`Capability::Rights(String)`) although `MailboxRights` exists. Owner:
`MailboxRights::parse`, produced by the decoder, expanding legacy letters per
RFC 4314 2.1.1 at parse time and exposing the one projection; pim delegates.

## CON-030 - What does an IMAP server event mean for sync?

**Reported by:** imap-protocol.

Three vocabularies for one stream: `TypedEvent` (connection), `IdleEvent`
(`connection/idle`) and `EventImpact` (`types/events.rs`, reached only through
the crate-private `ergonomics.rs`). `EventImpact` and `push::map_idle_event` each
decide what an event means and disagree: `Recent` -> `None` vs
invalidate-selected; NOTIFICATIONOVERFLOW -> `NotifyRegistrationLost` vs coarse
`Unknown`. `TypedEvent::MetadataChange` and `ServerMetadataChange` are never
constructed in production but every mapping handles them. Owner: one meaning
layer (the account's); delete or rewire `EventImpact`, and make the TypedEvent ->
IdleEvent step lossless (STR-057).

The imap-connection hunter counted four classifiers in a chain:
`UntaggedResponse -> TypedEvent` (`impl From` in `typed_event.rs`), then
`TypedEvent::priority()` (`typed_event.rs`), `TypedEvent::impact()`
(`types/events.rs`, a different module from the type), then
`typed_event_to_idle_event` / `untagged_to_idle_event` (`idle.rs`), then
`map_idle_event` / `absorb_idle_event` / `idle_event_ending`
(`account/push.rs`). Further disagreements: METADATA - `impact()` maps
`TypedEvent::MetadataChange` to `SelectedMailboxChanged`, but that variant is
never constructed; real METADATA arrives as `TypedEvent::Extension`, becomes
`IdleEvent::MetadataChange`, and `map_idle_event` maps it to
`HintPayload::Unknown`. Queue overflow - `impact()` says `EventQueueOverflow {
dropped_count }`; the IDLE path turns it into `IdleEvent::Alert(String)`, which is
documented as RFC 3501 section 7.1 text that "MUST be presented to the user", so
a consumer honouring that shows the user an internal back-pressure message as if
the server said it. Priority and impact are two independent tables over the same
variants. `IdleEvent` duplicates `TypedEvent` variant for variant (plus Timeout,
Cancelled, ServerTerminated). Dead variants: `TypedEvent::MetadataChange {}` and
`ServerMetadataChange {}` (never constructed, mapped by both `impact()` and
`typed_event_to_idle_event`); `IdleEvent::Bye` is unreachable on the live path
(the prologue turns BYE into an error first) yet `account/push.rs` carries two
defensive arms and a long comment for it; `IdleTermination::ClientDone` before
DONE maps to `ServerTerminated`, "Shouldn't happen". Its owner: one `ServerEvent`
enum and one classification table (`fn class(&self) -> EventClass { priority,
impact, ends_idle_wait }`) in one module, with `IdleOutcome = Event(ServerEvent) |
Timeout | Cancelled | ServerTerminated`; the account then reads `impact`, not
variants.

## CON-031 - IMAP mailbox display <-> wire conversion under a mode

**Reported by:** imap-protocol.

Decided in `codec/decode/mailbox.rs::decode_mailbox_from_wire`,
`encode_mailbox_str` (the INBOX-prefix rewrite lives only here),
`MailboxName::child` (`utf8 ? leaf : encode_utf7(leaf)`), `MailboxName::parent`
and `from_account_id` (`from_utf8_lossy` or `decode_utf7`), and
`MailboxName::identity` (`encode_utf7` for canonical). Owner: a codec-side
`WireMode::{encode, decode}` pair that `MailboxName` calls, with the INBOX rule
(BUG-027) there or in `MailboxName` construction, not in the encoder alone. The
type-level part is STR-058.

## CON-032 - IMAP decoder side tables kept in step with the parsers by hand

**Reported by:** imap-protocol (latent).

Each summarizes other code with nothing forcing agreement:

- `decode/extensions.rs::starts_known_untagged_response`'s `DIRECT_KEYWORDS`
  vs the `alt` list in `parse_untagged_with_backing`: decides whether a parse
  failure is a connection-fatal violation or a tolerated unknown response; a new
  parser added to the `alt` without the keyword is silently tolerant.
- `decode/envelope_fetch.rs::has_closed_grammar` vs the arms of
  `fetch_attr_value_inner`: the same strict/tolerant gate for FETCH attributes.
  `MODSEQ` is listed as closed grammar while its arm swallows a malformed value.
- `flags_caps::response_code_has_overflow` (code names and widths) vs the
  `response_code_inner` arms; the mod-sequence ceiling is `MOD_SEQ_MAX = i64::MAX
  as u64` in the encoder and a literal `i64::MAX as u64` here (STR-052).
- ResponseCode names three ways - already producing a live defect, filed as
  BUG-042.
- `CommandKind::untagged_routing_key` is documented as "a summary of `classify`"
  checked pairwise by a test - the brief's exact anti-pattern; derive it from a
  classification table.
- `types/profile.rs`: production `rev2_baseline_includes` is a non-exhaustive
  `matches!`, while the test oracle `capability_matrix::in_rev2_baseline` is the
  exhaustive match forcing a decision for every new `Capability`. The guard sits
  on the copy; make production the exhaustive match and delete the oracle.
- `classification::classify` ends in `_ => { debug_assert!(false); Impossible
  }`, defeating exhaustiveness; every row is `(_, UR::X)`-shaped, so matching on
  the response first and the command second would let the compiler catch a
  missing row.

Owner: table-driven decode dispatch (keyword -> parser + strictness + numeric
width) and a classification table from which routing keys are computed.

## CON-033 - Should this IMAP outcome retire the connection?

**Reported by:** imap-connection.

Sites: `driver_task`'s `Run` arm (`(!refused_before_send && is_connection_fatal)
|| (is_err && session == Logout)`); the `Upgrade` arm (`is_connection_fatal ||
(is_err && Logout)`, no `refused_before_send`); the `Pipeline` arm
(`failure.is_some()`, regardless of variant); the `Idle` arm
(`is_connection_fatal` only, no Logout clause);
`upgrade::retire_after_failed_refresh` and the STARTTLS/COMPRESS paths calling
`apply_infrastructure_failure` directly; `Error::is_connection_fatal`, a variant
list overridden by phase (`Closed` from `live_state_refusal` is "fatal" by
variant yet framing-safe, which is why `refused_before_send` exists); and on the
handle side `abandoned` (set by `Pending::drop`, `InFlightGuard`),
`is_reusable` and the pool. The variant does not decide fatality; the framing
state does, and the read loops know it. Owner: every loop returns a typed `enum
Framing { Untouched, Synchronized, Desynchronized }` alongside its result, one
driver function decides retirement, and `is_connection_fatal` is deleted or
reduced to a default for errors raised outside a loop.

## CON-034 - What does a foreign tag mean? Six IMAP read loops answer differently

**Reported by:** imap-connection.

The `OK/NO/BAD -> Ok/no_with_code/bad_with_code` match is written out in
`wire_send::wait_for_continuation`, `idle::run_committed_idle`,
`idle::wait_for_idle_grant`, `idle::drain_idle_responses`, `SelectConsumer`,
`NotifySetConsumer`, the fetch consumers and `dispatch/auth.rs`, beside the owner
`TaggedResponse::require_ok` that 24 consumers use - harmless as code. The
decision actually duplicated is foreign-tag policy: `dispatch_response_loop`,
`wait_for_continuation` and `wait_for_idle_grant` treat a foreign tag as fatal
`Protocol`; the IDLE loop and `drain_idle_responses` "ignore to preserve the
connection"; `logout_best_effort` ends the drain. Only one command is ever
outstanding in all six, so a foreign tag is the same fact (desynchronization) in
each. The loops (`dispatch_response_loop`, `route_pipeline_response` with two
callers, `wait_for_continuation` in two modes, `wait_for_idle_grant`, the IDLE
loop, `drain_idle_responses`, `logout_best_effort`) each re-implement read with
the right utf8 mode and budget, apply side effects, run the untagged prologue,
then a tagged/continuation/greeting policy; the untagged prologue was already
extracted after drift (`process_untagged_prefix`, `process_untagged_as_event`),
the tagged half was not. `logout_best_effort` also bypasses the "one sender"
(`send_wire_command`'s doc: "Every command ... reaches the socket through here")
by writing `format!("{tag} LOGOUT\r\n")` raw, and is the one tagged arm that
emits no response-code events. Owner: one `ReadLoop` driven by a `LoopPolicy {
on_own_tag, on_foreign_tag, on_continuation, route_untagged }`, which also makes
literal budgets apply in every loop (BUG-037) and gives CON-033's `Framing` one
producer.

## CON-038 - How does a JMAP push object decompose into state changes and token-free notifications?

**Reported by:** jmap-protocol (the two traversals already differ).

`event_source/stream.rs::flatten_push_object`: a worklist over `Group` that
MERGES every `StateChange` into one notification carrying the block's resume
token, emits `CalendarAlert` and `EmailPush` as their own notifications first,
and documents why that ordering is load-bearing. `sync/push.rs::collect_push` +
`last_push_state`: recursion over `Group`, routing each state change per account,
sending `CalendarAlert` and `EmailPush` to the `_ => HintPayload::Unknown` arm (a
broad reconcile - so if a server delivers alerts on the WebSocket push channel,
every fired alert costs a full repoll); `last_push_state` takes the last
`pushState` in depth-first order. Its doc says the SSE traversal is "the same
traversal" as the WS one; it is not. The `StateChange` shape is declared three
times: `PushObject::StateChange`, `client_ws.rs::WebSocketStateChange` (needed for
the `@type` framing), and `event_source::Changes`. The SSE lane is unwired, which
is where this drifted (STR-096). Owner: one `PushObject::decompose(self) ->
PushBlock { notifications, state: Option<MergedStateChange>, push_state }` in the
crate root (or `core/push.rs`), consumed by both lanes, with
`WebSocketStateChange` decoding straight into the shared struct.

## CON-042 - JMAP data type, capability and method name are restated at every invocation

**Reported by:** jmap-protocol.

`DataType` (crate root) is a string table parsed by hand; `Object` types do not
know their `DataType`; each `define_*_method!(EmailGet, Email, "Email/get",
capability::Mail)` restates the type name and capability, five or six times per
object; `sync/push.rs::scope_for_data_type` maps `DataType` back to cursor scopes.
`tests.rs::method_name_and_capability_table` is the pairwise-agreement test
holding the copies in step. Owner: `Object` gains `const DATA_TYPE: DataType` (or
`TYPE_NAME`) and `type Cap`; the macros derive `NAME` (`concat!(stringify!..)`)
and `Cap` from the object, and the table test becomes unnecessary.

## CON-043 - JMAP wire property names are declared twice per enum

**Reported by:** jmap-protocol.

Most object `Property` enums declare each wire name in a `#[serde(rename =
"...")]` and again in a hand-written `Display` match (mailbox, email_submission,
principal, push_subscription, calendar, address_book, sieve, vacation, identity,
...). `Display` is what `SetError<O::Property>::to_string_error` and `properties`
lists use; serde is what decode uses. `define_open_property_enum!` already
generates both from one table for calendar event and contact card; using it (or a
closed variant) everywhere removes the second answer, and the per-enum round-trip
tests in `tests.rs` that exist only because there are two answers.

## CON-045 - How many ids per JMAP call?

**Reported by:** jmap-account.

Computed in five places: `capabilities::validate_session` (`unwrap_or(1).max(1)`
after proving `> 0`); `capabilities::build` (`batching_policy.max_items =
max_objects_in_set.clamp(1, 500)`, `max_wait = 100ms`);
`mutation::mutation_stream` and `mutation::versioned_stream` (each `clamp(1, 500)`
again); `factory::max_objects_in_get` / `factory::max_objects_in_set`, which re-read
the session (with the same `unwrap_or(1)` fallback) for "doors that hold a bare
Account" (`pim.rs` single doors and `filters::list`); and `hydrate::ROUTE_MAX_WAIT`,
a local constant whose comment says it duplicates the advertised `max_wait`
because threading it was inconvenient. The factory is the wrong home for a
per-request limit, and doors "hold a bare Account" because the handle does not
carry its `CoreLimits` (BUG-057). Owner: the limits live on the per-account slot
(STR-097), one reading.

## CON-046 - The pagination guard: five copies, two opposite orders

**Reported by:** graph (both sides call their order deliberate).

Graph's `paging::PageWalk`, and in Google `changes::walk_refusal`,
`inventory::inventory_walk_refusal`, `contacts::address_book_walk_refusal` and the
inline loop in `calendar::calendars_list`. All use a 10,000-page budget, a repeat
as contract violation, and the budget as `LimitExceeded`. Graph checks the budget
FIRST (`the_budget_is_checked_before_the_repeat`: "the classification does not
depend on which link the server happened to hand out last"); Google checks the
repeat FIRST (`walk_refusal`: "a repeated token is the sharper diagnosis"). The
`PageWalk` module doc cites the Google fix as the reason for its own design.
Owner: one pure `PaginationGuard<K: Hash + Eq>` in `bifrost-types` (or
`bifrost-net`), with one ruled order.

The google hunter reports the four Google copies from inside: each doc comment
says it "matches" the other three, and the calendar copy differs in shape - its
budget is the `for` bound, so it refuses only after fetching the 10,000th page
rather than before following its token. `PageRefusal` is the shared result type,
but there is no shared guard. Its shape: `PageWalk { seen: HashSet<String>, pages:
usize, budget: usize }` with `admit(next: Option<&str>) -> Result<Option<Token>,
PageRefusal>`, in bifrost-net beside `parse_retry_after_hint`.

## CON-047 - May this URL carry the account credential?

**Reported by:** graph.

net attaches the bearer to the first request whatever its host (stated in Graph's
`origin.rs` module doc) and leaves admission to each crate: Graph (`origin.rs`)
uses an exact-origin `AdmittedUrl` type and refuses everything off the base;
dav-core (`DavDispatch::admit_discovered_urls` / `is_trusted_url`) uses a string
allowlist consulted per request, admits a cross-origin HTTPS discovered home, and
`resolve_url` decides "absolute" by the case-sensitive literal prefixes `http://`
and `https://`; JMAP relies on the redirect policy's `same_origin()`. The
transport attaches the credential, so it should own admission: `AccountSpec`
carries trusted origins, bearer-carrying requests take an `AdmittedUrl` minted by
net, and each crate contributes only its policy for widening the set (DAV's
discovered homes, Graph's upload sessions). Related: BUG-075.

## CON-048 - After a whole-submission failure, is an item Failed or Uncertain?

**Reported by:** graph.

Graph's `mutate::append_batch_failure` scans the cause chain for an `InFlight`
attempt or a `GraphError::Json` (the latter redundant with
`AttemptCause::unreadable_success`); Google (`mutation.rs`, the
`acknowledged_bodies`/`in_flight` match) and IMAP (`mutate.rs`,
`Verdict::Uncertain`) each have their own verdict; `RecoveryClass::requires_reconciliation`
already answers the question centrally. Owner: `ItemOutcome::from_submission_error(item,
error)` in `bifrost-types`, picking the lane from the error's own transmission
evidence.

## CON-049 - The final idempotency override is decided by three crates, and Graph can widen what net narrowed

**Reported by:** graph, net-dav.

The net-dav hunter frames it cross-crate: net narrows with
`idempotency_override(false)`; DAV narrows three more times for the same request
(in `dispatch_once`, in `send_raw_request`, and in `RedirectHistory::attach`); Graph
re-stamps after net and `apply_idempotency_override` replaces the override, its
comment admitting a widening context is valid only if every request was declared
`Idempotent`, which nothing enforces. Type fix: carry the caller's override on
`NetErrorContext` so net composes it, and make the builder override monotone (a
`narrow_idempotency()` that cannot widen, or an explicit
`widen_idempotency(proof)` requiring evidence).

A Graph request's replay safety is declared by the `ReplayPolicy` at the call
site; its recovery idempotency separately by `GraphErrorContext::idempotent()`,
re-applied by `apply_idempotency_override` after net translates. The doc on
`idempotent()` admits a widening context would overwrite a narrowing net stamped,
guarded only by `translate_exchange_ids_is_declared_idempotent`. Owner: the
declaration. net already carries the declared policy into the error
(`may_have_committed`), so a `ReplayPolicy::Idempotent` request could widen
recovery itself, with no classification-site override.

## CON-050 - Graph's two mail write pipelines each re-decide the same rules

**Reported by:** graph.

`mutate.rs` (bulk doors) and `pim/messages.rs` (typed doors) each build If-Match
PATCH, move and delete legs, submit `$batch` and reconcile. Decided twice:
cross-mailbox move refusal (`request_for_mutation` returns `None` and the caller
files `invalid_item_error`; `move_messages` checks it explicitly, its comment
saying "exactly as the bulk-move path in `mutate.rs` refuses the same condition");
public-folder write refusal (`mutate` partitions with `ews_read_folder` plus
`is_known_public_folder` on the destination; `pim` uses `refuse_public_write` per
id in `resolve_target_ids`, again in `move_messages`, and again in
`message_batch_url`, so on `add_to_container` the move-into-public-folder check
runs twice); "move carries the observed etag as a legacy guard"; and the etag
preflight (`refresh_missing_etags` in mutate; `fetch_message_value` and
`message_values_for_ids` in pim). Owner: one write planner,
`plan_message_write(target, effect) -> Result<BatchLeg, ItemRefusal>`, used by both
doors, which then differ only in reporting (streaming `ItemOutcome` vs
`PartialCommit`). The same two-pipeline shape is STR-080 (IMAP) and BUG-071
(JMAP).

## CON-051 - What does a call on a closed account return?

**Reported by:** google (cross-crate; the crates already differ).

Google refuses calls on a closed account as `Unsupported(op)`
(`account_closed_error`, `push::closed_error`) - the class a consumer reads as
"this account cannot do this", for a method `capabilities()` advertises. Graph's
`push::common::account_closed_error` refuses as `Internal(RuntimeFailure)`.
bifrost-types has no lifecycle kind for this, so each crate picks one. Owner: a
lifecycle kind (or a stated existing one) in the shared error model. Related: IMAP's
four lifecycle carriers, STR-083.

## CON-052 - Is a Gmail `NotFound` dischargeable under the cursor bridge?

**Reported by:** google.

The reason absence is not owed - the cursor is anchored at the historyId sampled
before the walk - is written out as prose at two sites that then act differently:
the inventory walk drops the id silently (`continue`); `repair_inventory` mints
`DefinitivelyIrrelevant::AbsentUnderCursorBridge`. Both test `account_error.kind()
== NotFound(Message)` after translation. The bisection and per-item lanes in
`mutation.rs` answer "is this a 404" a third way, from the raw status (`is_not_found`
on `response_parts`), and `MutationProgress::finish_failures` takes that answer as a
`not_found: bool`. Owner: one `fn absence_proven_by_cursor_bridge(&AccountError) ->
Option<Evidence>` beside `cursor.rs` (the proof rests on the cursor model), called by
both inventory sites; the walk should arguably report the same evidence rather than
silently `continue`, so the engine sees it. Related: BUG-021.

## CON-060 - Two redirect walkers, two 401-refresh-once paths, two deadlines

**Reported by:** net-dav (the copies differ in policy).

`request.rs::send_streaming_inner` (net) and dav-core `DavDispatch::send_raw_until`
with `RedirectHistory`, `rebase_destination` and `mint_auth_until`. DAV's stated
reason for a second walker is that net strips `Authorization` on an origin change
and cannot restore it - a missing hook in net's API, not a reason for two walkers.
Policy differences: method rewriting (net rewrites 301/302 on unsafe methods and
every 303 to GET; DAV preserves the method on 301/302/307/308 and refuses to follow
303); origin comparison (net `same_origin(&Url, &Url)`; DAV `url_origin(&str) ->
String`; a third rule in `dav_href_key`); the hop cap (kept in step only by both
naming `DEFAULT_MAX_HOPS`, a constant shared by convention); the deadline (DAV keeps
its own `Instant + DAV_CLIENT_TIMEOUT`, hand-maps expiry to `NetError::Timeout {
Unsent }`, and also sets `request_timeout` on the spec, while `RequestDeadline`
would own it but is `pub(crate)`). Owner: net's walk, parameterized by a per-hop
policy trait - `admit(prior, next) -> HopDecision` carrying the credential to
present (re-minted per origin) or a refusal; the method-rewrite mode (RFC 7231
rewrite, or preserve-and-refuse-303); a header rewrite hook (the DAV `Destination`
rebase); the dialect target guard. DAV's `send_raw_until`, `RedirectHistory`, its
401 handling and its deadline then disappear. This is the exchange core of STR-151.

## CON-061 - Throttle-scope stamping happens at three sites for DAV alone

**Reported by:** net-dav.

net `status_kind_cause` returns a throttle bool for 429/507; DAV `status_error`
stamps `ThrottleScope::Account` on 429 and 507 itself; DAV `net_account_error`
passes `Some(Account)` into net's funnel; DAV `dispatch_once` re-stamps `Account`
afterwards on any `RateLimited` or `QuotaExhausted` kind; Graph's `stamp_budget`
re-stamps after net; JMAP passes `Account`. The test
`generic_transport_throttle_matches_dav_status_policy` exists to hold two DAV copies
in step. Owner: the one status classification (BUG-072) returns `throttles: bool`,
and the provider context supplies the scope (and Graph's budget) exactly once.
Related: BUG-083, CON-013.

## CON-062 - "Is this entity tag weak?" is answered at five sites

**Reported by:** net-dav.

The DAV half landed: one parser in `dav-core/src/etag.rs`, `EntityTag` and a
private-field `StrongTag`, and `PutCondition::IfMatch` requires the strong tag.
Residue: the validator still travels as `String` in `ServerVersion::ETag(String)`
(bifrost-types) and at the DAV account and snapshot boundaries, with the weak
marker kept as text so later code re-detects it. Moving `EntityTag` into
bifrost-types and adopting it for Graph's `If-Match`, Google People and Google
Calendar etags is the remaining owner change.

## CON-067 - The "restart paging" page-cursor fault is built in three places

**Reported by:** wave 3 reconciler.

`Request(Malformed)` with `InvalidArgument { field: Some("page_cursor") }` is the
documented signal to restart a page walk from page one, and it is constructed
separately in IMAP (`account/pim.rs`) and JMAP (`sync/error.rs` and
`sync/page_cursor.rs`). One constructor in bifrost-types would make it one
definition every provider calls.

## CON-065 - Google Drive and OneDrive drive the same resumable-upload protocol separately

**Reported by:** net-dav (the copies disagree).

Google's `upload_file_chunked_progress` and Graph's `upload_chunks` /
`admitted_resume_offset` repeat window arithmetic, the `bytes {offset}-{end-1}/{total}`
header, resume-offset admission, final-chunk tracking for the partial-commit
record, and redirect refusal. They disagree on a resume that made no progress
(Google: `Internal(LimitExceeded)`, "Drive documents resending"; Graph:
`ContractViolation`) and on the attempt budget (Google has a total budget; Graph has
none and relies on strictly increasing offsets). Owner: a resumable-upload driver in
bifrost-net taking a provider closure to parse the server's resume offset, with the
provider-specific no-progress rule an explicit parameter, not a fork.

## CON-066 - "Send to a pre-authenticated session URL" is three bools that must agree

**Reported by:** net-dav.

`without_bearer_auth().credential_url().follow_redirects(Disabled)` is the same
triple at three sites: Graph `execute_aux` for `AuxTarget::Anonymous`, and Google
`cloud.rs` twice. Forgetting one leaks a token, leaks the URL into diagnostics, or
replays a chunk to a redirect target. Owner: `AccountNet::session_request(method,
&SessionUrl)` with a `SessionUrl` newtype whose `Debug` and `Display` redact.
