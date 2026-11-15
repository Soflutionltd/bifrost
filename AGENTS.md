# bifrost

## Project

`bifrost` - Cargo workspace of Rust clients for email/calendar/contact protocols, built for [ratatoskr](https://github.com/folknor/ratatoskr).

Crates present:

- `crates/jmap/` → `bifrost-jmap` - JMAP client. RFC 8620 / 8621 / 8887 / 9404 / 9425 / 9610 / 9670, calendars draft-26, sieve draft-14.
- `crates/imap/` → `bifrost-imap` - IMAP client. Daaki-derived driver-task model. Tokio + native-tls only.
- `crates/google/` → `bifrost-google` - Google account client. Gmail mail, People contacts, and Google Calendar are wired.
- `crates/graph/` → `bifrost-graph` - Microsoft Graph account client.
- `crates/carddav/` → `bifrost-carddav` - CardDAV account crate for contacts.
- `crates/caldav/` → `bifrost-caldav` - CalDAV account crate for calendars.
- `crates/smtp/` → `bifrost-smtp` - SMTP and LMTP client. Lettre-derived. Native-tls only.
- `crates/sasl/` → `bifrost-sasl` - private shared SASL/SCRAM computation layer (SCRAM, CRAM-MD5). Consumed by `bifrost-imap`; not public.

All crates are pre-1.0, API stabilization phase.

CalDAV composes into IMAP-shaped accounts when configured.

### Who this is for: the product yardstick

ratatoskr is an enterprise mail, calendar and contacts client - think Outlook in
a corporate deployment - and it does what bifrost says it should. There is no
separate consumer contract to discover: when a behaviour question has no
technical answer (what a vanished target means for a move, whether a partial
failure is reported or absorbed, which address a message is sent from, how loud
an error should be), decide it by asking **what an enterprise Outlook user would
expect to see**, and implement that. A user who moves a message deleted elsewhere
wants it gone from view, not an error and not a false "moved"; a user sending on
behalf of a shared mailbox expects the mailbox on the envelope; a user who loses
network expects a retry, not a demand to re-enter their password.

Such a decision needs no owner ruling. Record it where it lives - a comment at
the code that implements it, and the crate's `reference/` doc where a consumer
relies on it - stating the choice and the user expectation that settled it, so a
later reader can disagree with a reason rather than rediscover the question.

## Rules

### General rules

- Don't use gremlins! Em-dash, en-dash, strange quotes, whatever - they're all verboten.
- Don't remind the user of the rules. They wrote them, so they know them.
- The user can exempt you from any rule at any time.

### Bash rules

- Never read or write from `/tmp`. All data lives in the project.
- Never run raw `cargo`, `curl`, `pkill`. Use `brokkr`.

### Testing rules

The line is hermeticity, not size. A test belongs here if it is deterministic and runs entirely in-process.

- In scope: parser and encoder tests, type-level and object-safety checks, validation rules, error classification and recovery mapping, serde round-trips, state-machine sequencing driven through in-crate test doubles, and byte-level protocol transcripts fed through an in-memory duplex.
- Test doubles are fine and encouraged: `StubAccount`-style fakes implementing the crate's own traits (see `crates/imap/src/account/test_support.rs`), stub transports, canned server transcripts. These are not "integration tests" - no socket, no port, no daemon.
- Out of scope, still: real sockets or listeners, fixed ports, Docker, live credentials, external accounts, wall-clock sleeps, anything that can fail because a network did.
- Public APIs are exercised against real servers downstream, not here. Bifrost does not prove its protocol round-trips against live endpoints.
- Prefer the smallest test that pins the behavior. Breadth is welcome where it is hermetic; ceremony is not.
- Coverage is not a target in itself. Do not propose growing the suite to "match coverage of similar crates" - but do not treat the current size as a ceiling either.

## Commands

Use `brokkr` (not `cargo`) for check/test. `brokkr man` is the authoritative
documentation and ships with the binary; read it rather than trusting a
restatement, including this one.

### Sweeps

`brokkr.toml` declares these `[[check]]` sweeps, and every `brokkr check` and
`brokkr test` runs all of them:

- `workspace` - every member, default features. Cargo unification across the
  workspace selection donates the non-default features in-workspace consumers
  ask for (`bifrost-jmap/sync` via bifrost-sync, `bifrost-smtp/tokio` via
  bifrost-imap, `bifrost-net/test-support` via dav-core's dev-dependency).
- `smtp-dkim` - `bifrost-smtp` with `dkim`, the one optional feature no member
  enables.
- `jmap-calendars-off` - `bifrost-jmap` with its default features minus
  `calendars`, pinned to `feature_unification = "selected"`. This is the only
  leg that typechecks `#[cfg(not(feature = "calendars"))]` code and the
  exhaustiveness arms that exist only for a calendars-off build.
- `jmap-no-default` - `bifrost-jmap` with no features at all, pinned to
  `feature_unification = "package"`: resolved per package, the way
  `cargo install` resolves, so no sibling member can donate a feature back.
  Every default feature of this crate is optional, so the zero-feature build is
  a promise to consumers.
- `smtp-no-default` - `bifrost-smtp` with no features at all, pinned to
  `feature_unification = "package"`. `account-error` is a default feature that
  gates a large part of the crate, and the code that only compiles with it off
  is typechecked nowhere else.
- `smtp-no-default-tokio` - `bifrost-smtp` with no default features plus
  `tokio`, pinned to `feature_unification = "package"`: the async transport
  without the account-error layer.
- `jmap-sync` - `bifrost-jmap` with its non-default `sync` feature, pinned to
  `feature_unification = "package"`. A focused `brokkr test -p bifrost-jmap`
  otherwise never compiles `crates/jmap/src/sync/`, since only bifrost-sync
  donates the feature; package resolution also proves `sync` enables
  everything it needs itself.
- `install-shape` - every published crate at its default features, pinned to
  `feature_unification = "package"`, so each builds and passes its tests the
  way a downstream dependent resolves it. Its `packages` list must be kept in
  step with the workspace's published members. It runs on every check: its
  warm cost was measured small enough to keep in the gate, and it roughly
  doubles the test count because each crate's suite runs again under its own
  resolution.

Declaring `[[check]]` entries replaced brokkr's implicit single `--all-features`
sweep, so these sweeps are the coverage, not an addition to it. A new optional
feature that no workspace member enables is unchecked until a sweep names it.
`feature_unification = "package"` is the tool for the other blind spot: a
crate that compiles in the workspace only because a sibling donates a feature
(the blocking SMTP transport once failed to build without `tokio` for exactly
that reason); `install-shape` applies it to every published crate. `brokkr man config check` documents every sweep key.

Validate a `brokkr.toml` key by running brokkr before writing it into any
document: a config-load failure is raised before any phase runs, so it costs
nothing. A key once propagated from a stale brokkr help string into this file,
then into a filed ruling, with nothing in the loop checking it against the tool.

### brokkr check

- `brokkr check` - the full pipeline, in order: gremlins, header, textlint,
  manifest, script_check, dependency_rules, publish_cycle, clippy, tests.
  Each phase short-circuits the next. Doctests run too (`[test] doctests = true`).
- A tree holding **only markdown edits** runs the gremlins, textlint and
  script_check phases and stops; pass `--force-rust` to run clippy and the tests
  anyway.
- Output is never capped or scoped: every diagnostic prints, every time.
  Errors in files with unstaged changes are listed first.
- `--gate` exists but needs a `[test] gate_profile` in `brokkr.toml`, which
  this project does not define.
- `brokkr check -p <crate>` - scope every sweep to one package; repeatable. The
  set is intersected with each sweep's own scope, so a sweep that admits none of
  the named packages is skipped. You generally do not want this; a single
  `brokkr check` is faster than 2-3 `-p` runs.
- `brokkr check -- --test <file>` - forward args to `cargo test`. The leading
  `--` is required, and args after a *second* `--` go to libtest
  (`brokkr check -- -- --ignored`).
- `--timings` - after the check, list every test that ran by descending wall
  time. `--json` appends one machine-readable summary line. `--fix-gremlins`
  rewrites banned Unicode in place before scanning.
- A `cargo test` that succeeds having run zero tests fails the phase, so a
  too-narrow filter cannot green-light a check.

### brokkr test

`brokkr test -p <crate> <NAME>` - focused single-test runner. Always passes
`--include-ignored --nocapture --test-threads=1` (nightly is a prerequisite).
Profile comes from `[test] debug` in `brokkr.toml`, which is currently `true`,
so it builds dev by default. `<NAME>` is a case-sensitive substring filter
matching both unit and integration tests within the package. Streams the test's
own stdout/stderr live and prints a `[test] PASS/FAIL/BUILD FAILED/SKIP` footer
per sweep with wall time.

A sweep reports `SKIP` when the name matched nothing in it (feature-gated out)
or when the `-p` target is outside that sweep's `packages` - both expected here,
since every sweep but `workspace` is single-crate. `SKIP` alongside at least one
`PASS` exits 0; *every* sweep skipping means the name was wrong and exits
non-zero.

- `-p, --package <PKG>` - cargo package. Required in this workspace - no
  `[test] default_package` is set and there is no built-in default.
- `-N, --repeat <N>` - run the test N times per sweep (flaky-test hunting).
- `-j, --jobs <N>` - parallel cargo compile jobs.
- `--sweep <LABEL>` - run only one sweep (`--sweep workspace`,
  `--sweep jmap-calendars-off`) instead of all of them.
- `--raw` - bypass output filtering, print everything cargo emits.
- `--debug` / `--release` - force a profile. `--debug` is redundant while
  `[test] debug = true`; it matters only if that is flipped back.
  `BROKKR_TEST_BIN_DIR` points at the matching `target/` subdirectory.
- `--timeout <SECS>` - raise the per-test watchdog, 1-280s. Brokkr kills any
  test running past 20s by default, here and in `brokkr check`'s test phase;
  this flag is the only exception, and it only applies when `<NAME>` resolves to
  exactly one test.
- Example: `brokkr test -p bifrost-jmap session_roundtrip` or
  `brokkr test -p bifrost-sync backfill_checkpoint -N 5`.

## Code style

Workspace-wide conventions. Per-crate conventions live in `reference/<crate>.md`.

- No per-file copyright headers; attribution lives in `README.md`.
- Async-only (no maybe_async, no blocking).
- `#[non_exhaustive]` on all public enums.
- Clippy lints in workspace root `Cargo.toml` `[workspace.lints.clippy]`.
- Don't commit ad-hoc reference docs outside `notes/` or `reference/`.
- Code comments must never point to documents in `notes/`. Notes move, get renamed, and get deleted; source comments that name a note path rot silently and require cross-tree edits when the doc moves. Explain the "why" in the comment itself, or point at a stable doc under `reference/`.

## Reference

Per-crate architecture and conventions. Single source of truth for current code state; in-flight design lives in `notes/`.

**Before working in a crate, read its `reference/<crate>.md` first.** These docs are kept in sync with the code and exist precisely so agents do not have to rediscover module layout, trait surfaces, or invariants from scratch. Skipping the read is how mistakes that the reference would have flagged get made.

- `reference/error-model.md` - cross-cutting `AccountError` contract every protocol crate produces and `bifrost-sync` reads: opaque builder funnel (`try_build` invariants), `AccountErrorKind` + message-key namespace, `RecoveryClass` and the central `derive` mapping, cause chain + transmission evidence, three-lane `BatchOutcome`, diagnostics consent tiers. The shared target the per-crate error mappings below map onto.
- `reference/jmap.md` - bifrost-jmap dispatch, transport, module pattern, capabilities, error model, and the `Account` impl under `crates/jmap/src/sync/` (cursor envelope, inventory / changes / hydration, WebSocket push, mutation pipeline, recovery taxonomy).
- `reference/jmap/` - the JMAP sub-references `jmap.md` cites: `DEFERRED.md` (unimplemented spec work and accepted deferrals, including the EventSource push fallback), `API.md` (archived ADR for the pre-1.0 surface redesign), and the per-RFC implementation plans `MDN.md`, `SMIME.md`, `SHARING.md`.
- `reference/imap.md` - bifrost-imap driver model, cancellation safety, streaming FETCH, typed IDs, auth, and the account layer under `crates/imap/src/account/` (QRESYNC / CONDSTORE / Basic cursor strategy, per-folder modseq cache, opportunistic `STORE UNCHANGEDSINCE`).
- `reference/google.md` - bifrost-google `Account` impl: Gmail history-id seeded sync, Cloud Pub/Sub push with renewer and health stream, mutation pipeline with flag canonicalization and TRASH fallback, and Google People contacts.
- `reference/graph.md` - bifrost-graph `Account` impl: Microsoft Graph delta-token sync, webhook push with renewal health worker plus EWS streaming fallback, cursor envelope and validation, `If-Match` etag mutations, error mapping.
- `reference/carddav.md` - bifrost-carddav standalone contact Account implementation.
- `reference/caldav.md` - bifrost-caldav standalone calendar Account implementation.
- `reference/smtp.md` - bifrost-smtp transport types, PIPELINING, DSN, message builder, LMTP.
- `reference/net.md` - bifrost-net shared HTTP transport: retry, rate-limiting, observability.
- `reference/sasl.md` - bifrost-sasl private SASL/SCRAM computation crate: `Secret`, `SaslError`, `ScramHash`, the pure SCRAM/CRAM functions, and the error-mapping contract with the protocol crates.
- `reference/sync.md` - bifrost-sync engine: scheduler, multiplexer, partitioned backfill, push reconciler, mutation pipeline, checkpoint envelope versioning, scope lifecycle.

## Document folders

The standing layout, across every project. Three live folders plus one retired,
split by durability first, subject second.

| Folder | Contents | Rule |
|---|---|---|
| `reference/` | Durable in-repo reference for anyone working on or with the code - how the thing is built and why: `architecture.md`, `technical-implementation-spec.md`, `performance.md` (the durable record of measured numbers over time), invariants, protocol contracts | Citable from source as a source of truth. What it says must be true. |
| `docs/` | Durable in-repo documentation of how the thing is used - guides, CLI reference, the consumer-facing API surface. Sometimes exposed as a hand-edited VitePress gh-pages site | Same must-be-true rule. |
| `notes/` | Transient - work items (`todo.md`), future plans, hypotheticals, bug reports, research, analysis. Things that will die | No truth guarantee. Nothing durable cites it. |
| `plans/` | Retired | Plan documents are transient: they go in `notes/`. |

`reference/` and `docs/` are both durable and both binding. The difference is
subject, not audience: `reference/` covers how the thing is built and why - what
you need in order to change it safely - while `docs/` covers how it is used. A
developer or library consumer reads both. Where a project publishes a site,
`docs/` is what gets published; the folder means the same thing either way.
`notes/` is neither durable nor binding, which is the whole point of keeping it
separate: a document that may be wrong must not sit where a document that must
be right is expected.

The dependency direction is therefore one-way. `notes/` may cite `docs/` and
`reference/`; nothing durable may cite `notes/` - not a code comment, not
`docs/`, not `reference/`. A code comment must carry its full context, because
it outlives the note.

**Root-level convention files are exempt.** `AGENTS.md`, `CLAUDE.md`,
`README.md`, `LICENSE`, `CHANGELOG.md` and their kin are found by tooling and by
convention at the repository root, and stay there. These folders govern
documents we chose where to put, not files whose location is dictated.

In `notes/`, `docs/` and `reference/` alike, avoid citing source line numbers -
they drift fast.

## Standing lessons

Things this project has paid for. Preserved from the bug-hunt arcs of August
2026, whose per-scope ledgers were closed out and deleted once their durable
content reached `reference/` and the code.

### Never delete a capability on your own judgment

This is the most expensive mistake made here, and it was made twice in one
session: one commit deleted `bifrost-smtp`'s entire blocking transport half
(`SmtpTransport`, `LmtpTransport`, `Transport`, the blocking pool, the seven
blocking examples, the `tokio` feature), and another deleted `bifrost-sync`'s
`Scheduler`, `ConcurrencyBudget`, `SchedulerConfig`, `MutationConfig`,
`LiveSupersedes`, `BackfillCheckpointWriter` and `mutation::fanout`. Both were
restored at the owner's instruction. Three separate things went wrong:

- **"Nothing calls it" was established by grepping this workspace.** For a
  library crate that is close to meaningless - its consumers are outside the
  workspace by definition. "The only in-workspace consumer is async" is a fact
  about the workspace, not about who uses `SmtpTransport`. Likewise "documented
  as deliberately unwired, with no dated plan" describes somebody's plan; a
  missing date is not evidence of abandonment.
- **Findings documents mix four kinds of entry under one heading level**: live
  defects, latent defects, refactor opinions, and product decisions about what
  the published surface should be. Only the first two are bugs. Standing
  procedure pushes toward action, so an aesthetic judgment written into a bug
  document gets laundered into a mandate. Separate the defects from the
  proposals before working any list.
- **Deletion was often not even the only fix on offer.**
  `MutationConfig::retry_queue_cap` reading as a bound while the queue was an
  unbounded `Vec` is a real defect - and "make the field actually cap the queue"
  is at least as good an answer as "delete the field". Where a finding proposes
  removing something, look for the fix that keeps it.

The rule: **a change that removes a capability entirely - something a consumer
could do that would no longer be possible - stops and asks the repository
owner**, no matter which document recommends it and no matter how confident the
argument. "Nothing in the workspace calls it" is never the argument that settles
it. "Build, don't defer" settles build-versus-defer. It does not settle
delete-versus-keep.

Breaking the published API is ruled fine (2026-11-15), and needs no ruling:
renaming, moving an item between modules or crates, narrowing an engine-internal
item's visibility, collapsing duplicate types, reshaping fields, variants and
signatures, and dropping an alias, shim or redundant second door to something
that stays reachable. No compatibility variant or shim is kept for the old shape.
The crates are pre-1.0. The test is whether the consumer can still do the thing,
not whether the old spelling still compiles.

### The recurring defect shape is a fix that opens a new hole one layer up

Check what a fix does to its consumer, not only to the unit test in front of it.
This is measured, not impressionistic: across two arcs it fired ten times, and
every one was caught by a cold review or a close pass, never by the fix pass's
own tests. The sharpest cases rhyme - fixing a `PageBoundary::Final` contract gap
introduced a hydration deadlock; fixing a push-watch leak made `close()`
cancellation-unsafe, creating a different unreclaimable leak; and the drop guard
added to fix *that* was constructed inside the async block, so a future dropped
before its first poll leaked the same registration one poll earlier. A fix aimed
at a resource leak produced a new resource leak three times running.

**When the change is to teardown, ordering, or a lifetime, assume the hole moved
rather than closed, and go looking for where.**

The corollary: a cold reviewer's value is entirely in its ignorance. It is the
only stage that does not share the author's priors, and it has a perfect record
on this defect shape. Every sentence of context added to that prompt is a prior
installed in the one reviewer that should not have any.

### Audit new tests for bite, mechanically

Revert the production change, confirm the test fails, restore. Three tests here
have been caught passing against the bug they were written for - most recently an
entire "exhaustive" alias-pair suite that passed against the pre-fix code, which
also proved the finding it came from was never a defect.

- **Uniform inputs are how a concurrency test fails to bite.** A FIFO admission
  test whose waiters all had cost 1 passed against a governor with no queue at
  all, because single-threaded scheduling order alone reproduced the answer. It
  only bit once the head was made expensive and the follower cheap, so an
  overtake was observable. Same shape as the 500,000-element test that never
  produced a single-element result.
- **Check the name filter before believing a PASS.** `brokkr test -p X <NAME>` is
  a substring match, and a filter matching none of the tests you meant reports
  PASS. Two ablation runs were read as "the test does not bite" when the test had
  simply not run.

### A refactor that MOVES a field must move the tests that pinned it

The `NetConfig` split deleted three tests along with the fields they described,
which was locally correct and left three defaults silently unpinned. A falling
test count after a refactor is the signal; chase it rather than accepting it.

### Fixing a reader is not finished until the OTHER readers of the same identity are checked

The 2026-07-31 workspace sweep hunted one failure mode: a local restatement
of a rule that lives somewhere else, kept alive by a test that exercises the
copy rather than the original. The copy and its test agree indefinitely; only
the original disagrees, and nothing asks it. It found two live defects, both
the same shape: a producer's shape had moved, one consumer followed, and a
sibling arm in the very same `match` showed the correct treatment while the
arm beside it silently dropped to `_ => None` or a default.
`resolve_throttle_key` read only `ErrorScope::Mailbox` while every production
folder producer builds `Cursor(Folder(_))`, so `ThrottleKey::Mailbox` was
unreachable in production and its test passed against a shape only tests
build; and both engine inventory front ends absorbed `InventoryEvent::Warning`
one arm below the `Terminated` case that forwards. Three method notes:

- **Delete the copy, do not read it.** Reading the copy tells you what it
  claims; deleting it (or routing ONE test through the real path) tells you
  whether the claim was true. Three of four calibration defects were found
  that way.
- **Drift needs movement.** Greps for `mirrors` / `same shape as` mostly
  surface copies of FROZEN specs, which cannot rot. Look where a producer
  changed shape, a seam was promoted, or a contract crossed a layer boundary.
- **Arm the compiler instead of reading.** `#![warn(dead_code)]` sits on the
  error-translation boundaries (`crates/imap/src/account/error.rs`,
  `crates/jmap/src/sync/error.rs`), where a dead item is a hole in a contract.
  Removing the crate-wide `allow(dead_code)` from imap and jmap outright is
  not a cheap win: measured at 105 and 195 warnings, overwhelmingly
  legitimate unused protocol surface.

### Verify with a full `brokkr check`, never `-p`

Scoped runs miss cross-crate breakage, and feature unification makes them
non-equivalent to the real thing. A fix pass that verifies with `-p <crate>` has
not verified.

### A smaller mechanic

- **`git add -N` every untracked source file before a cold review.** A reviewer
  reads the unstaged diff, so a round whose central deliverable is a new module
  gets reviewed with that file invisible. This has happened.

### A review loop needs a scope fence and a stopping rule

The bounded backfill lane (September 2026) went through seventeen cold-review
rounds overnight under the instruction "fix all lateral findings before
committing". Rounds 1 to 4 built the feature that was ruled on. Rounds 5 to 17
built a second feature - a durability guarantee the new receiver sweep had made
visible - that nobody had ruled on, and each round's fixes handed the next
reviewer more surface, so the series never converged: the last round still
found a P2. Two rules follow.

- **A lateral finding is FILED, not fixed inside the change that surfaced it.**
  This is the "separate the defects from the proposals" lesson above, applied
  to reviews: a reviewer's finding outside the ruled scope goes into
  `notes/todo.md` with its evidence and gets its own ruling. Building it in
  place lands a product decision nobody made.
- **An unattended loop stops on a rule stated up front.** Only P1 and P2
  findings are fixed, everything else is filed, and a round that finds no P1 or
  P2 ends the loop. "Fix everything the reviewer finds" is an instruction to
  continue until the reviewer tires, and reviewers do not tire.
