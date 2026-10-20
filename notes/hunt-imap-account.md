# Hunt: bifrost-imap account layer

Scope: `crates/imap/src/account/`. Read against `reference/imap.md` (Account
layer, Mutations, Push lifecycle, Per-folder mutation failure contract),
`reference/error-model.md`, `reference/sync.md`.

Findings are ordered roughly by severity. Each names the claim it breaks.

---

## 1. Mutation folder-preparation failures are always `Uncertain`, contradicting the documented lane rule

`mutate.rs` `flush_mutation_groups`, the `Err(FolderMutationFailure { .. })` arm.

Every `FolderMutationFailure` is raised in `open_folder_for_mutation` (checkout,
SELECT, UIDVALIDITY read, MODSEQ-cache fold), before any STORE / MOVE /
EXPUNGE is sent. The arm nonetheless mints `ItemOutcome::Uncertain` for every
id, unconditionally.

Claims broken:

- `reference/imap.md`, "Per-folder mutation failure contract": "a per-item
  outcome for every remaining target in that folder (... `Uncertain` on
  `InFlight` transmission evidence, `Failed` otherwise)". The code has no
  `Failed` branch here.
- `reference/imap.md`, "Mutation error lanes follow transmission evidence ...
  `Unsent` or `Acknowledged` evidence makes them `Failed`", and
  `mutation_error_verdict`, which already implements that rule and is not used
  on this path.
- `reference/imap.md`, PIM primitives: preparation failures are stamped
  `Unsent` "relative to the operation", because the SELECT's own evidence is
  about the SELECT, "and publishing it would make recovery reconcile a
  non-idempotent mutation that never reached the server". The sync-side
  mutation path does the opposite: it is worse than publishing the SELECT's
  evidence, because it ignores evidence altogether and declares the
  mutation's outcome unknown.
- The comment in `flush_mutation_groups` itself ("the engine cannot tell,
  post-hoc, whether the mutation landed") is false for this lane. Nothing was
  sent.

Effect: a tagged `NO` to SELECT, a closed pool, a dial failure or a missing
UIDVALIDITY sends every id in that folder into the engine's read-back /
reconcile queue instead of the retry or fail lane. The fix is to stamp the
preparation error `Unsent` relative to the mutation (as `preflight_err` does in
`pim.rs`) and run it through `mutation_error_verdict`. Then there is one lane
rule, not two.

## 2. A shared-folder permission denial on mutation SELECT kills the whole mutation stream

`mutate.rs` `flush_mutation_groups`, `stream_terminating`.

`open_folder_for_mutation` errors are classified by plain `account_error_with`,
not `error::shared_folder_error`. A `NO [NOPERM]` (or `[AUTHORIZATIONFAILED]`)
SELECT on a shared or other-user folder classifies as
`Authorization(PermissionDenied)`. `stream_terminating` matches every
`Authorization(_)`, so the stream emits `Terminated` and returns. That leaves:

- every later folder group (groups are processed in name order) unattempted and
  unanswered;
- every target not yet pulled from the input stream undrained and unanswered.

Claims broken:

- `reference/imap.md`, "Mutation batch shape": "every submitted id is answered
  exactly once, across all three lanes".
- `reference/imap.md`, Shared folders: "Revocation quarantines, not
  escalates", and "`NO [ACL]` stays authoritative per-mutation". The changes
  and inventory paths route this exact denial through `shared_folder_error` to
  a scoped `ScopeRevoked`. The mutation path is the sibling reader that was
  not updated. This is the "fixing a reader is not finished until the other
  readers are checked" shape.
- `stream_terminating`'s own doc reserves `Terminated` for "authentication /
  authorization loss ... that prevent any further folder attempt". A
  per-folder ACL denial does not prevent other folder attempts.

Even for a personal folder, a per-folder `PermissionDenied` is not an
account-wide authorization loss. Consider narrowing `stream_terminating` to
`Authentication(_)` plus the sync-state breaks, and letting per-folder
authorization failures settle per item.

## 3. VANISHED ranges are expanded UID by UID, and every expanded UID becomes a `Removed` change even if the consumer never knew it

`changes.rs` `run_qresync` (SELECT-side `vanished` and the
`FetchStreamItem::VanishedEarlier` arm), `record_removed_change`;
`mutate.rs` `open_folder_for_mutation`; `push.rs` `absorb_idle_event`;
`folder_registry.rs` `expand_range`.

Two problems with one root cause:

a) Correctness. `record_removed_change` pushes a `ScopeChange::Removed` for
every UID in the range, whether or not it is in `known_uids`. The second
command, `UID FETCH 1:* (...) (CHANGEDSINCE m VANISHED)`, is issued over
`1:*`, not over the known set. RFC 7162 Section 3.2.6 lets that reply name any
UID expunged since `m`, including messages that arrived and were expunged
between syncs and that the client never saw. Servers also commonly coalesce
VANISHED into ranges that cover non-existent UIDs. The result is `Removed`
events for ids with no membership record. The CONDSTORE path explicitly guards
the symmetric hazard ("announcing `Updated` for an object the consumer has no
membership record of is the hazard the CONDSTORE path guards",
`basic_updated_change` doc; `reference/imap.md` CONDSTORE bullet), and QRESYNC
does not guard this one. The fix is to intersect each range with `live_uids`
(the baseline) before emitting anything.

b) Resource. `expand_range` materialises `start..=end` into a `Vec<u32>`. A
server reply of `VANISHED (EARLIER) 1:4000000000` (legal syntax, and well
within what a buggy or hostile server can send) allocates on the order of 16
GB and then emits billions of changes, in the changes stream, in every mutation
SELECT, and in every IDLE VANISHED event. `CompactUidSet` is already
range-based. Removal, the modseq-cache clear and the diff should all operate on
ranges intersected with the known set, never on expanded ranges.

The same expansion runs on `ResponseCode::Modified` in `modified_uids`
(`mutate.rs`), where the bound is at least the request's own set. It should
still be range-based.

## 4. `Account::close()` is not cancellation-safe or concurrency-safe, and leaves SMTP submission open

`close.rs`.

- `closed.swap(true)` is the first thing `close` does. If the close future is
  dropped after the swap and before `pool.close().await` finishes (the pool
  drain awaits LOGOUT under a timeout), every later `close()` returns `Ok(())`
  at once while sessions remain connected. A concurrent second `close()`
  likewise returns `Ok(())` while the first is still draining. Both break
  `reference/imap.md`: "`Account::close()` therefore means no session owned by
  that account is still connected when it returns". This is the standing-lesson
  shape: a teardown flag set before the teardown is done.
- `submission: Option<Arc<SubmissionTransport>>` is never closed. The SMTP
  transport is pooled (`SmtpSubmissionConfig::with_pool`). Its sessions stay
  connected past `close()`, and `send_message` / `send_raw_message` /
  `draft_send` never check `closed`. After `close()` they still dial SMTP and
  deliver mail. Only the Sent APPEND then fails, and that failure is only
  logged, so the caller gets `Ok` with a fallback `imapmsgid1:` id. A
  post-close send succeeding is the opposite of a closed account.

## 5. The push lane does not "reserve one slot": IDLE sessions are uncounted, so the connection footprint exceeds `pool_cap`

`factory.rs` (`data_cap = pool_cap - 1`), `pool.rs` `dial_idle`, `push.rs`
`ensure_idle_tasks`.

`data_cap` is `pool_cap - 1` "for push", but `dial_idle` takes no permit, and a
non-NOTIFY account starts `idle_connection_budget` (default 4) workers, each
with its own session. Defaults give 3 data sessions plus 4 IDLE sessions,
which is 7 sessions against a `pool_cap` of 4. Above that come
`replace_selected_member` re-dials.

Claims broken:

- `reference/imap.md` `pool.rs` bullet: "Push lane reserves one slot; data
  lanes share the rest."
- `reference/imap.md` Push lifecycle: "Four matches `pool_cap` ...
  deliberately: it gives INBOX plus three other hot folders real push coverage
  while keeping the account's total connection footprint inside the low
  per-user allowance many providers enforce." The total is not kept inside
  `pool_cap`. On a provider with a per-user limit of about 5, the eighth dial
  is refused, and that dial is usually a data checkout during sync.

Either the IDLE workers draw from a shared session budget, or the docs and
`ImapAccountConfig` docs must state the real total
(`pool_cap - 1 + idle_connection_budget`).

## 6. `unsupported_stream` emits `Done` after `Terminated`

`mod.rs` `unsupported_stream` (used by `open_blob`, `open_blob_range`).

The stream yields `Terminated(unsupported)` and then `Done(None)`. The
`SyncEvent` docs (`crates/types/src/events.rs`) say `Terminated` "signals that
this stream ends here". A consumer that treats `Done` as success after it has
already processed `Terminated` sees two terminal events. Every other
unsupported path in the crate (and `changes_stream`'s routing error) emits
`Terminated` alone.

## 7. The opportunistic MODSEQ cache can abort sync and mutation on a value it only caches

`folder_registry.rs` `FolderEntry::record_modseq`, its call sites in
`changes.rs`, `inventory.rs`, `mutate.rs` and `push.rs`.

`record_modseq` returns `Error::Protocol` (connection-fatal,
`ProviderContractViolation`) for `MODSEQ 0`, and every call site propagates it
with `?`. A cache whose documented purpose is "only ever saves a round trip on
`STORE UNCHANGEDSINCE`" (`MODSEQ_CACHE_CAPACITY` doc) therefore terminates an
inventory walk or a changes run, or fails a whole folder of mutations (into the
`Uncertain` lane of finding 1). The protocol-layer parser is the right place to
judge `MODSEQ 0`. The cache should skip the entry. The UID-0 and
UIDVALIDITY-0 checks are reachable only from local bugs and belong in
`debug_assert`s.

## 8. The MODSEQ cache records server state the consumer has not seen, so `UNCHANGEDSINCE` guards less than it appears to

`push.rs` `absorb_idle_event` (records `MODSEQ` from IDLE FETCH),
`mutate.rs` `open_folder_for_mutation` (records from the mutation's own SELECT
QRESYNC `changed_messages`), and inventory and changes recording as they
stream.

`UNCHANGEDSINCE` exists to reject a write based on a stale view. The cache is
advanced from any server read, including the mutation's own SELECT
immediately before the STORE and push events the engine has not reconciled
yet. So a concurrent change by another client that landed after the
consumer's last hydration is normally folded into the guard value just before
the STORE, and the STORE then succeeds over it. Capabilities honestly advertise
`MutationConcurrency::None`, so this is not a broken promise. It does mean
`reference/imap.md`'s "Concurrency conflicts and UNCHANGEDSINCE" section
describes a guard that, as wired, protects only the sub-second window between
SELECT and STORE. Either the guard value should come from the consumer
(fingerprint `ServerVersion::ModSeq` already travels in inventory entries), or
the docs should say plainly how narrow the window is.

## 9. Push: per-folder SELECT failure is reported as an account transport disconnect, and retried forever

`push.rs` `idle_loop`, the `conn.select(..)` `Err(_)` arm.

A subscribed folder that was deleted, renamed or ACL-revoked makes its worker
emit `WatchEvent::Disconnected` (which bifrost-sync turns into the
account-scoped warning "push transport disconnected"). The worker then dials,
authenticates and SELECTs again every 5 to 300 s for the life of the account.
Each retry is a full connect, which is what the comments elsewhere say
providers with connection-rate limits punish. A later successful round then
emits `Reconnected`, which the engine turns into a full account-wide
`HintPayload::Unknown` reconcile. Because `Disconnected` and `Reconnected` are
account-level signals multiplexed from per-worker state, worker B can emit
`Reconnected` while worker A is still down, and vice versa.

The SELECT error is discarded (`Err(_)`) rather than classified, so a
`NONEXISTENT` or `NOPERM` cannot be told apart from a network failure. This
does not break a written contract outright. It does break the push section's
premise that `Disconnected` means the transport went away.

## 10. QRESYNC runs a redundant second round trip over `1:*`

`changes.rs` `run_qresync`.

A QRESYNC SELECT (RFC 7162 Section 3.2.5) already returns `VANISHED (EARLIER)`
and the changed-message FETCHes since `modseq`. The code consumes both and then
issues `UID FETCH 1:* (UID FLAGS MODSEQ) (CHANGEDSINCE modseq VANISHED)`, which
returns the same data again. `fetch_change_seen` / `removed_seen` deduplicate
it. Cost: one extra full round trip per folder per poll on the cheap
strategy, plus finding 3a's widened VANISHED (over `1:*` instead of the known
set, since this command carries no known-uids operand). If the second command
is a deliberate guard for non-conformant servers, the reference should say so.
`reference/imap.md` currently lists both sources without explaining why both
are needed.

## 11. Mutation SELECT carries the full QRESYNC baseline on every mutation

`mutate.rs` `open_folder_for_mutation` passes the folder's in-memory cursor to
`select_folder`, so every mutation batch on a QRESYNC account sends the
complete known-UID set as the SELECT operand. The server then replies with every
VANISHED and every changed FETCH since the last sync. All of it is used only
to warm the MODSEQ cache, and the VANISHED part is expanded per UID (finding
3b). On a large mailbox after a long idle gap, a one-message flag flip pays
for a full resync payload. A plain SELECT is enough for a mutation.

## 12. Smaller observations

- `changes.rs` `run_qresync` / `finish_changes` call `folders.set_cursor`
  before the `Final` / `Done` send. An abort in between (abort-on-drop is the
  documented norm) advances the in-memory cursor past what the consumer
  checkpointed. Today it is read only for mutation SELECT parameters (finding
  11), so the effect is limited to cache warming, but the ordering is backwards
  for anything that might read it later.
- `mutate.rs` `expunge_uids_or_fall_back`: the `UID SEARCH DELETED` then plain
  `EXPUNGE` fallback is a check-then-act race. A `\Deleted` set by another
  client between the two commands is expunged. The doc's guarantee ("only run
  when ... the mailbox's `\Deleted` set is exactly the set this batch marked")
  holds at SEARCH time, not at EXPUNGE time. The reference should name the
  window, since it cannot be closed without UIDPLUS.
- `mutate.rs` ignores `IdempotencyKey` on all three bulk paths (`_key`). That
  is acceptable for IMAP, but nothing in `reference/imap.md` says so, while
  other providers' docs explain their key handling.
- `push.rs` `push_stream`: events broadcast while no `push_stream` receiver
  exists are lost (`broadcast::Sender::send` with zero receivers). A
  `push_subscribe` that starts workers before the engine attaches its stream
  loses the first events silently. bifrost-sync may always subscribe the stream
  first. If so, that ordering is an undocumented requirement.
- `changes.rs` Basic strategy cannot observe flag changes on a non-CONDSTORE
  server. The reference describes only additions and expunges for Basic, so
  this is honest, but `CursorDescriptor` for Basic does not signal "flags
  stale", and nothing in the engine-facing types says that a Basic folder's
  flags drift until re-inventory.
