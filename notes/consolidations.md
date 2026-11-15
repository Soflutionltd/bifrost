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

## CON-013 - Is this error a throttle, and for how long? (four-plus recorders, two hold stores)

**Reported by:** sync-engine (sites already disagree).

The engine half landed in wave 13: `AccountError::throttle_holds()` and
`HoldTarget` in bifrost-types cover every leg, and `ThrottleBucket::observe()`
(which owns its lock) is the one engine recorder. Residue is the cross-crate
bridge. The wave 13 fixer's reading, with evidence: bifrost-net should own HTTP
provider holds (Google records through `shared_hold` and installs `hold_gate`,
and net gates requests before the engine observes the error), while the engine
keeps account, mailbox and non-HTTP holds. The bridge between them is not
built, so a net local-hold error is still recorded into the engine bucket a
second time.

The original question: `bifrost-net` has its own hold store
(`SharedHold` and `hold_gate`, string-keyed, `HoldKind::{RateLimited,
QuotaExhausted}`, used by Google), and the engine has `ThrottleBucket` (keyed by
`ThrottleKey`, used through stamped budgets by Graph and IMAP). Both answer "is
this provider budget held right now" with different keys, clocks and lifetimes,
and an error from a net local hold is recorded into the engine bucket a second
time. Decide which store owns provider holds and feed the other from it.
Related: CON-009 (budget declared vs stamped).

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

## CON-020 - What does a push teardown refusal mean?

**Reported by:** sync-engine.

`SyncEngine::unsubscribe_push`, `subscribe_push`'s refused-registration cleanup,
`reattach.rs::unwind_replacement_subscriptions`, `retire_stranded` and
`orphan_teardown.rs` each call `push_unsubscribe`, decide whether a refusal
becomes an orphan, and log. Only the unwind path records throttle holds
(through `RepairPacing`); only the retrier applies a timeout. Owner: a
`SubscriptionTeardown` service on the registry, with "delete, settle or orphan,
record holds" as one operation.

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

## CON-047 - May this URL carry the account credential?

**Reported by:** graph.

Partly landed in wave 15: `crates/net/src/url.rs` has `TrustedOrigins` and a
net-minted `AdmittedUrl`, Graph and DAV admit through them, and an optional
`AccountSpec::trusted_origins` is checked before automatic bearer lookup.
Residue: an `AccountSpec` without `trusted_origins` still relies on each
protocol's own admission, because composed handles and JMAP session endpoints
have no complete account-wide authority set yet, so bearer-carrying requests
are not required to take an `AdmittedUrl`. The original account:

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

## CON-052 - Is a Gmail `NotFound` dischargeable under the cursor bridge?

**Reported by:** google.

The two inventory sites now share one evidence-producing helper in
`account/cursor.rs`, and repair reports its evidence. Residue: the inventory
walk still discharges a proven absence silently, because `InventoryEvent` has
no per-object evidence variant, so the engine never sees the proof. That needs
a shared inventory API change in bifrost-types. (Mutation 404 handling stays
separate on purpose: it proves target absence, not inventory coverage.)

