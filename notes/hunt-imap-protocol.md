# Hunt: imap-protocol (wire grammar and typed protocol values of bifrost-imap)

Scope read: `crates/imap/src/codec/` (decode, encode, classification, utf7),
`crates/imap/src/types/` (all files), `crates/imap/src/error.rs`, `lib.rs`.
Followed into `connection/` (driver, helpers, idle, typed_event, auth,
mailbox, extensions, search_validation) and `account/` (envelope, poll,
folder_registry, pim, push, get, inventory, mutate, error, sieve) only as far
as needed to see how the protocol values are consumed, plus `bifrost-sasl`,
`bifrost-smtp` authentication and `bifrost-types` mime header parsing for the
cross-crate questions. The decoder internals were read at the dispatch and
gate level, not line-audited grammar by grammar.

Findings are tagged **[defect-live]**, **[defect-latent]**, **[shape]**
(axis or placement proposal, not a bug) so the lists can be worked
separately.

---

## The one-paragraph thesis

The protocol layer already owns a good typed vocabulary - `Uid`, `Seq`,
`UidValidity`, `ModSeq`, `UidSet`, `SeqSet`, `SearchCriteria`, `FetchAttr`,
`StatusItem`, `MailboxRights`, `AuthMechanism`, `ObjectId`, `ImapAtom`,
`GmailMessageId` - and almost none of it survives to the place that acts on
it. Values are rendered to `String` / `u32` / `u64` one hop before the
encoder (`SyncSelectOptions::qresync_params`, `Command { items: String,
criteria: String, .. }`) and the encoder or connection then re-parses and
re-validates the string (`SequenceSet::new_known` on a known-uids string,
`validate_mod_sequence_value`, a 250-line SEARCH grammar walker that exists
only to rediscover which keys the builder wrote). On the response side the
decoder produces raw `u32` / `Option<u32>` / `String`, and every consumer
re-decides "is 0 valid", "is this range reversed", "is this a UID or a
sequence number", "is this mailbox INBOX". Most of the multi-site decisions
below are downstream of that one choice: render early, re-parse late. The
biggest payoff is a rewrite that carries typed values from builder to wire
and from wire to consumer, with the encoder as a pure renderer and the
decoder as the one place wire tolerance is decided.

### Ranked moves

1. One IMAP set algebra type for UID / sequence sets (A1), replacing six
   representations and their disagreeing normalization rules.
2. One `ConnectionCaps` value owning `(advertised, enabled)` and every
   question derived from it, with `enabled` typed (A10, D4). Fixes the live
   QRESYNC gate disagreement.
3. Typed AST in `Command` (SEARCH keys, FETCH attrs, STATUS items, QRESYNC
   params, NOTIFY groups, dates) so the encoder renders instead of
   re-validating, and capability requirements come from the AST (A5, A7, A8,
   A14).
4. One mailbox identity rule (INBOX canonicalization, wire mode) owned by
   `MailboxName` plus a codec-side `WireMode` (D1, D9, A16).
5. SASL vocabulary, payloads and policy moved wholly into `bifrost-sasl`,
   including the ManageSieve path, which currently hand-builds an
   injection-unsafe XOAUTH2 payload (D5).
6. Decoder dispatch made table-driven so strictness, numeric width and
   recognized-keyword sets cannot drift from the parsers (D10).
7. Fix the dependency direction: `types` and `codec` must not import
   `connection` or `account` (S1); move ManageSieve out of `account/` (S2).

---

## 1. Axes that should be types

### A1. Six representations of "a set of UIDs / sequence numbers" **[shape] + [defect-latent]**

Representations, each with its own invariants:

- `SequenceSet(String)` (`types/validated.rs`) - validated string; `new_known`
  rejects `*`/`$` by `contains`.
- `UidSet(SequenceSet)` / `SeqSet(SequenceSet)` (`types/ids.rs`) - typed
  wrappers whose constructors format strings and `expect` the validator;
  `coalesce_numbers` and `UidSet::from_ranges` are two coalescers.
- `UidRange` (`types/uid_range.rs`) - public `start: u32`, `end: Option<u32>`
  plus two hidden `*_is_star` bools; `single(n)` and `range(n, n)` are
  distinct values for the same set; `Default` is the invalid zero UID;
  `range` only debug-asserts non-zero; `try_range` permits reversed ranges.
  The same type carries sequence numbers (ESEARCH ALL, MODIFIED for STORE)
  and UIDs.
- `ParsedUidSet(Vec<(u32, u32)>)` (`types/validated.rs`) - re-parses a
  `SequenceSet` string, maps `*` to `u32::MAX`.
- `CompactUidSet { ranges: Vec<UidRange> }` (`account/folder_registry.rs`) -
  the real set algebra (contains, insert, remove, remove_range, diff, count,
  iter), plus `normalized_range`, whose doc comment states it exists only
  because `UidRange` has two spellings of one UID and `PartialEq` would
  otherwise disagree.
- `expand_uid_ranges` (`connection/mod.rs`) - its own sort/merge/expand.

Plus the account cursor codec (`account/envelope.rs`) encodes a single UID as
`end = 0` sentinel, and `bifrost-types` `InventoryPartition::Uid` is half-open
`u64`.

The decision "what does a reversed range mean" is made independently and
already disagrees:

- decoder `uid_range` / `seq_range`: swap endpoints (Postel);
- `ParsedUidSet::new` and `intersect_uid_ranges`: swap;
- `CompactUidSet::from_ranges`: DROP (`end >= start` filter);
- `CompactUidSet::remove_range`: swap (`min`/`max`) and clamp to 1;
- `expand_uid_ranges`: DROP ("names no messages");
- cursor decoder: REJECT as schema-incompatible;
- `account/mutate.rs` and `connection/dispatch.rs`: `min`/`max` swap inline.

Today the decoder normalizes first so most consumers never see a reversed
range, but every `UidRange` built through `try_range` or the public fields
can reach a consumer reversed, and the consumers disagree on what it means.

Proposal: one value type, e.g. `types::set::UidSet` = sorted, disjoint,
maximally merged `RangeInclusive<Uid>` (no dual spellings, no zero, no
reversed), with the set algebra now in `CompactUidSet` moved onto it, a
`SeqNumSet` twin over `Seq`, and a separate command-operand type
`SetExpr { Set(UidSet), All, SavedResult, ..WithStar }` for the wire-only
`*` / `$` forms. The decoder produces `UidSet` (normalizing once), the
encoder renders it, the cursor serializes it, and the one conversion to the
engine's half-open `u64` partition lives beside it. `CompactUidSet`,
`ParsedUidSet`, `normalized_range`, `expand_uid_ranges`'s merge, and both
coalescers disappear.

### A2. UID versus sequence number is not in the response types **[shape]**

`UntaggedResponse::Search { uids: Vec<u32> }` ("sequence numbers or UIDs"),
`Sort { nums }`, `ThreadNode::id: Option<u32>` ("UID (or sequence number)"),
`EsearchResponse { uid: bool, min/max: Option<u32>, all: Vec<UidRange> }`,
`ResponseCode::Modified(Vec<UidRange>)` ("STORE seqs, UID STORE UIDs"),
`Expunge(u32)`, `Removal::Seq(u32)`, `FetchResponse { seq: u32, uid:
Option<u32> }`. The typed `Uid` / `Seq` exist and are unused here. Whether a
number is a UID is decided by each consumer from which command it remembers
issuing. The command already knows: make the addressing a type parameter of
the result (`SearchResult<Uid>` vs `SearchResult<Seq>`), parse the ESEARCH
`UID` indicator into the type rather than a bool, and have the decoder hand
`MODIFIED` to the consumer as a raw set the consumer types by its command.

### A3. Typed identifiers that never reach their consumers **[shape]**

- `ImapAtom`, `validated::ObjectId`, `GmailMessageId`, `GmailThreadId`,
  `types::Address` and the `EnvelopeAddress -> Address` conversions are used
  only by their own tests (hidden by the crate-root `allow(dead_code)`).
- `Uid` / `UidValidity` / `ModSeq` reach two places (`account/targets.rs`,
  `account/mod.rs::select_options`). Everywhere else is raw: `ResponseCode::
  UidValidity(u32)`, `AppendUid { uid_validity: u32 }`, `SelectedMailbox {
  uid_validity: Option<u32>, uid_next: Option<u32>, highest_mod_seq:
  Option<u64>, mailbox_id: Option<String> }`, `FetchResponse { mod_seq:
  Option<u64>, email_id: Option<String>, thread_id: Option<String>,
  gmail_msg_id: Option<u64> }`, `FolderCursor { uidvalidity: u32, modseq: u64
  }`, `DecodedObjectId { uidvalidity: u32, uid: u32 }`, `ModSeqCache {
  uidvalidity: Option<u32>, by_uid: HashMap<u32, u64> }`,
  `Error::FetchLimit { seq: Option<u32>, uid: Option<u32> }`.
- The PUBLISHED surface uses bare `NonZeroU32` for two swappable siblings:
  `ImapMessageRef::new(container, uid_validity: NonZeroU32, uid:
  NonZeroU32)`, `UidScan { uid_validity, from, through }`, `UidPage {
  completed_through, next_from }`, `FolderObservation { uid_validity,
  uid_next }`, `RawMessage::GenerationChanged { observed }`. Nothing stops a
  caller passing a UID where a UIDVALIDITY goes. Publishing `Uid` and
  `UidValidity` and retyping those fields is a shape change (ruled fine, no
  removal involved).
- Two typed views of the same `(folder, uidvalidity, uid)` triple:
  `DecodedObjectId` (raw `u32`) and public `ImapMessageRef` (`NonZeroU32`),
  the latter rebuilding from the former with `expect("the id decoder rejects
  zero")` - an invariant held by another function's behaviour, not a type.
  One `MessageRef` with typed fields would serve both.

### A4. "Is a zero UIDVALIDITY / MODSEQ / UIDNEXT usable" has no owner **[defect-latent]**

The decoder accepts 0 for UIDNEXT, UIDVALIDITY, UNSEEN, HIGHESTMODSEQ, MODSEQ,
APPENDUID and COPYUID uidvalidity ("Postel, Dovecot sends 0 for empty
mailboxes"). Then:

- connection SELECT folding stores `UidValidity(0)` as `Some(0)` although the
  `SelectedMailbox::uid_validity` doc says `None` exists to avoid the invalid
  sentinel 0; HIGHESTMODSEQ 0 there is treated as NOMODSEQ;
- `account::selected_uidvalidity` (documented as "the one place the
  classification lives") checks presence only and returns `Ok(0)`;
- `poll::observation` additionally rejects 0;
- `folder_registry::record_modseq` silently skips uidvalidity 0 and modseq 0;
- `select_options` drops QRESYNC when `UidValidity::new(cursor value)` is
  `None`;
- the encoder rejects uidvalidity 0 and modseq 0 in QRESYNC as
  `InvalidInput` (a caller-blaming class);
- `decode_object_id` rejects 0 via `parse_nonzero_u32`.

Consequence on a server that reports UIDVALIDITY 0: inventory, changes and
hydration mint object ids `imap2:..:0:<uid>` through `encode_object_id`, and
`decode_object_id` rejects every one of them, so nothing inventoried can be
hydrated or mutated; the folder cursor is written with uidvalidity 0, so
every reconnect silently drops QRESYNC and full-resyncs. Single owner: the
decoder (or the SELECT consumer) turns the wire number into
`Option<UidValidity>` / `Option<ModSeq>` once, with the "0 means absent"
tolerance stated there, and every downstream field is the typed value.

### A5. QRESYNC parameters typed, then stringified, then re-validated **[shape]**

`SyncSelectOptions { uid_validity: Option<UidValidity>, mod_seq:
Option<ModSeq>, known_uids: Option<UidSet>, seq_match_data: Option<(SeqSet,
UidSet)> }` is converted by `qresync_params()` into `QresyncParams {
uid_validity: u32, mod_seq: u64, known_uids: Option<String>, seq_match_data:
Option<(String, String)> }`; the connection handle then checks
"seq-match requires known-uids", and the encoder checks it again, plus
non-zero uidvalidity, the modseq range, CRLF in the set strings, and
re-runs `SequenceSet::new_known` on them. Make `Command::Select` carry
`Qresync { validity: UidValidity, modseq: ModSeq, known: Option<KnownUids {
uids: UidSet, seq_match: Option<(SeqNumSet, UidSet)> }> }`: seq-match
without known-uids becomes unrepresentable, every encoder check but
rendering disappears, and the stringly `QresyncParams` plus the
`SelectOptions`/`SyncSelectOptions` pair collapse into one type.

### A6. The mod-sequence range is a constant in two places and a type in none **[shape]**

`MOD_SEQ_MAX = i64::MAX as u64` in the encoder (`validate_mod_sequence_value`
/ `_valzer`), a literal `i64::MAX as u64` width for HIGHESTMODSEQ / METADATA
in the decoder's `response_code_has_overflow`, and `ModSeq(u64)` with no
constraint. `Command::{Fetch,UidFetch}::changed_since` and
`{Store,UidStore}::unchanged_since` are `Option<u64>`. A `ModSeq` constructor
that enforces 1..=2^63-1 (and a `ModSeqValzer` admitting 0 for
UNCHANGEDSINCE) owns the rule; both checks then go away.

### A7. `Command` is stringly typed, so the encoder re-parses the builder's output **[shape]**

`Fetch { items: String }`, `Status { items: String }`, `ListStatus {
status_items: String }`, `Search { criteria: String }`, `SearchReturn {
return_opts: Vec<String> }` (SAVE detected by `trim().eq_ignore_ascii_case`),
`Enable { capabilities: Vec<String> }`, `Authenticate { mechanism: String
}`, `ListExtended { selection_options, return_options: Vec<String> }`,
`GetMetadata { depth: Option<String> }`, `SetAcl { rights: String }`, `Sort
{ sort_criteria, charset }`, `Thread { algorithm, charset }`,
`NotifyEvent::MessageNew { fetch_attrs: Vec<String> }`, `AppendMessage {
date: Option<String> }`.

Typed sources exist for most of these (`FetchAttr` + `format_fetch_attrs`,
`StatusItem`, `SearchCriteria`, `AuthMechanism`, `MailboxRights`,
`Capability`). The sharpest cost: `SearchCriteria` is a `String` builder, so
deciding which capability a SEARCH needs requires
`ImapConnection::search_criteria_contains_atom`, a hand-written SEARCH
grammar walker (`connection/search_validation.rs`) that the ENCODER calls
through `crate::connection` to rediscover whether the builder wrote
`MODSEQ`, `OLDER`, `SAVEDBEFORE`, `EMAILID` or `$`. Quoting is also decided
twice: `search_helpers::quote_imap_string` always quotes and rejects CR/LF at
build time (before the connection's UTF-8 mode is known), while
`CommandWriter::string` chooses quoted-vs-literal from the live mode.
Proposal: `SearchKey` as an enum AST (with an explicit `Raw { text,
requires: Vec<Capability> }` escape hatch), rendered by the encoder through
`CommandWriter::string`, with `SearchKey::required_capabilities()`; the
grammar walker, `validate_search_criteria_crlf` and `write_criteria`'s
literal re-emission go away for typed criteria.

### A8. IMAP dates: two validators that disagree, one formatter, no parser **[defect-live] (minor)**

- `search_helpers::validate_imap_date` (SEARCH SINCE/BEFORE/ON): month
  compared case-SENSITIVELY against `"Jan".."Dec"`, no day-of-month check.
- `encode::validate_append_datetime` (APPEND): month case-INSENSITIVE (RFC 3501
  Section 9 says alphabetic characters are case-insensitive), day-vs-month and
  leap-year checks, hour/minute/second/zone ranges.
- `account/pim.rs::imap_date` formats a `SystemTime` into the string that the
  first validator then re-parses.
- `FetchResponse::internal_date` / `save_date` stay raw `String`; nothing
  parses INTERNALDATE.

So `"13-feb-2025"` is rejected for SEARCH and accepted for APPEND, and
`"30-Feb-2025"` is accepted for SEARCH and rejected for APPEND. `jiff` is
already a dependency: `ImapDate` / `ImapDateTime` types with `Display` for
the encoder and a decoder-side parser own both directions.

### A9. One `Flag` type for read-only and writable flags **[shape] + [defect-live]**

`Flag` includes `Recent` and `Wildcard` (`\*`, PERMANENTFLAGS-only) beside
writable flags, and `Custom(String)` is unvalidated. "Which flags may a
STORE / APPEND carry" is decided at `encode::validate_and_filter_flags`
(silently drops `\Recent` / `\*` with a debug log, validates keyword atoms)
and again at `connection::filter_store_flags`; `account/inventory.rs::
imap_flag_target` separately special-cases `\recent` by string. Split into
`StoreFlag` (system flags + validated `Keyword(ImapAtom)`), `Flag`
(observed, includes `Recent`) and `PermanentFlag` (includes `Wildcard`).

Related live disagreement: translating a provider-neutral keyword into an
IMAP flag. `pim::imap_flag_for_keyword` folds `$seen` / `$flagged` /
`$answered` (JMAP spellings) onto `\Seen` / `\Flagged` / `\Answered`;
`account/mutate.rs` uses plain `Flag::from` (no folding); the flag judge
compares raw strings. So `set_keyword("$seen")` stores `\Seen`, while
`bulk_set_flags({"$seen"})` stores a literal keyword `$seen`, and a
read-back of the latter judges against `\seen`. One owner,
`Flag::from_neutral_keyword` in `types/flag.rs`, used by every write path
and by the judge.

### A10. ENABLEd extensions are `Vec<String>`; the UTF-8 mode is a bool **[shape] + [defect-live]**

`enabled: Vec<String>` lives in `ProtocolState`, the connection snapshot,
`EncodeOptions` and `ServerProfile`, fed by `UntaggedResponse::Enabled(Vec<
String>)` and sent via `Command::Enable(Vec<String>)`. "Is UTF8=ACCEPT
enabled" is a string scan at `driver::utf8_mode`, `EncodeOptions::
utf8_accept_enabled`, `helpers::check_utf8_only_enforced`,
`helpers::utf8_enabled` and the ENABLE path in `extensions.rs`; "is QRESYNC
enabled" at `mailbox::validate_qresync_params` (whose comment says it matches
"exactly as `ServerProfile::enabled` does" - agreement held by a comment)
and `uid_ops`; `imap4rev2_active` scans for `"IMAP4rev2"`. `profile.rs`'s own
doc says the `(capabilities, enabled)` pair is owned by four views and asks
callers to go "through a typed wrapper" that does not exist.

Proposal: `ConnectionCaps { advertised: CapabilitySet, enabled:
EnabledSet }` (enabled entries typed as `Capability`), one value held by all
four views, owning `supports`, `imap4rev2_active`, `binary_fetch_usable`,
`utf8_mode`, `literal_mode`, `qresync_enabled`. Replace the ~60 `utf8_mode:
bool` / `utf8: bool` parameters across decode and encode with an enum
`WireMode { Ascii, Utf8 }` (also the bool inside `MailboxName`, A16).
`ServerProfile::auth_mechanisms: Vec<String>` and `thread_algorithms:
Vec<String>` duplicate `Capability::Auth` / `Thread` and should be views.

The live consequence is in D4 (QRESYNC gate).

### A11. Custom equality to paper over a non-canonical representation **[shape]**

`Capability`, `Flag` and `MailboxAttribute` each hand-write `PartialEq` and
`Hash` so that `Other("IDLE") == Idle`, `Custom("\\Seen") == Seen`,
`Custom("\\Noselect") == NoSelect`. That exists only because the escape
variant can hold a known name. Make the payload a private newtype minted only
by `from_imap_str` (which already canonicalizes), derive equality on a
case-folded key, and the cross-representation arms go. `Capability::hash`
allocates a `String` per call (`as_imap_str` returns `String`).
`Capability::SortDisplay(String)` only ever holds `"DISPLAY"`.
`MailboxAttribute` has no `Inbox` variant, so `pim` matches
`Custom("\\Inbox")`.

### A12. ACL rights travel as strings **[shape]**

`MyRights { rights: String }`, `AclEntry { rights: String }`, `ListRights {
required: String, optional: Vec<String> }`, `Command::SetAcl { rights: String
}`, `Capability::Rights(String)`. `MailboxRights` exists; the decoder should
produce it. See D3 for the semantic duplication.

### A13. Invariants stated in prose on result types **[shape]**

`MoveResult::copy_uid` / `CopyResult::copy_uid: Option<ResponseCode>` are
documented "Always a `ResponseCode::CopyUid` when present". Every consumer
re-matches. Use `Option<CopyUid { validity: UidValidity, source: UidSet,
dest: UidSet }>` (and the same struct inside `ResponseCode::CopyUid`).

### A14. NOTIFY event-group legality checked at send time **[shape]**

RFC 5465 dependency rules (MessageNew iff MessageExpunge; FlagChange and
AnnotationChange require both; fetch attributes only on `selected`) are
checked by `encode::commands::notify::validate_notify_event_group` and
asserted again in `account/push.rs` tests. A shape such as `MessageEvents {
new_fetch: Vec<FetchAttr>, flag_change: bool, annotation_change: bool }`
makes the illegal group unrepresentable and removes both.

### A15. Failures and signals that reach callers only as prose **[shape] + [defect-latent]**

- `Error::MissingCapability(String)` carries free text (`"QRESYNC (not
  ENABLEd)"`, `"SELECT (CONDSTORE) requires CONDSTORE"`, `"AUTH=OAUTHBEARER"`).
  The account maps it to `Unsupported` with support-only text, so neither
  the account layer nor a consumer can tell which capability, or advertised
  versus not-enabled. Carry `Capability` plus a reason enum.
- `ValidationError(String)` -> `Error::InvalidInput(e.to_string())`: every
  local validation reason is prose.
- `AuthPolicyFailure { offered: Vec<String>, rejected: Vec<..AuthMechanism..>
  }`: one half typed, one half strings.
- `TypedEvent::QueueOverflow { dropped_count }` (a data-loss signal) is
  converted by `idle::typed_event_to_idle_event` into
  `IdleEvent::Alert(format!("event queue overflow: {n} events dropped"))`,
  i.e. an operator alert string. `push::map_idle_event` maps every `Alert`
  to a coarse `Unknown` invalidation, so the loss is handled safely today only
  because Alert happens to get the coarse treatment; the typed fact is gone.
  Likewise metadata changes become `ExtensionEvent("METADATA change during
  IDLE")` and unrecognized responses become `ExtensionEvent(format!("{other:
  ?}"))` - Debug output as an event payload.
- `TypedEvent::NotificationOverflow { code: Option<String> }` although
  `ResponseCode` exists.

### A16. `MailboxName`'s wire encoding is a bool, and identity is recomputed per comparison **[shape]**

`MailboxName(String, Option<(Vec<u8>, bool)>)`, `identity() -> (bool,
Vec<u8>)`, `wire_bytes(utf8: bool)`. `identity()` runs `encode_utf7` and
allocates on every `eq` and every `hash`, so each `HashMap<MailboxName, _>`
probe UTF-7-encodes the name; `can_reencode()` (called by `child`, `parent`,
`account_id`, `matches_mailbox`, the encoder) computes two identities. The
canonicality bit is recomputed by round-trip because `decode_utf7` returns a
lossy `String` with no "was this canonical" signal. Have the codec return the
canonical flag, store `enum Wire { Canonical, Raw { bytes, mode: WireMode }
}` and a precomputed identity key.

### A17. Body section names are strings **[shape] + [defect-latent]**

`BodySection { section: String, origin }` and `FetchAttr::BodySection {
section: Option<String> }`. `account/get.rs::merge_fetch_response` matches
body sections case-insensitively by name and IGNORES `origin`, so two partial
fetches of the same section at different origins in one response collapse
(the second is dropped); binary sections compare `Vec<u32>` exactly. A typed
`SectionSpec { parts: Vec<u32>, text: Option<Header | HeaderFields(..) |
Text | Mime> }` plus origin as part of the key owns "same section".

---

## 2. Decisions made in more than one place

### D1. Is this mailbox INBOX / the same mailbox? Six answers **[defect-live]**

- `MailboxName::eq` / `hash`: wire identity, NO INBOX folding (`"inbox"` !=
  `"INBOX"`).
- `MailboxName::matches_mailbox`: folds exact `INBOX` only.
- `classification::mailbox_names_eq`: folds exact `INBOX`; production-dead,
  kept alive only by `connection/helpers_tests.rs` through the `inbox_eq`
  alias - a copy that exists to be tested.
- `encode::commands::encode_mailbox_str`: folds `INBOX` AND rewrites an
  `INBOX<ascii-separator>child` prefix to upper case.
- `account/push.rs::first_inbox_folder`: case-insensitive, with a
  tie-break because the registry can hold two INBOX-equivalent entries (a
  direct consequence of the first bullet).
- `account/pim.rs::folder_role` name fallback: leaf-name `"inbox"`, so
  `Archive/Inbox` or `Shared/alice/INBOX` classifies as the Inbox role (see
  L1).

Disagreement that bites: a caller names `inbox/Sub`; the encoder sends
`INBOX/Sub`; the server answers `* STATUS "INBOX/Sub"`; `matches_mailbox`
compares `inbox/Sub` with `INBOX/Sub`, says no; `classify` routes the solicited
STATUS as unsolicited; the STATUS consumer's finalize reports the missing
STATUS as `ProtocolMissing`. Same shape for QUOTAROOT, ACL, MYRIGHTS,
METADATA and LIST-correlated consumers. Owner: `MailboxName` construction
canonicalizes the INBOX token (and, if the encoder's prefix rule is kept,
the INBOX-prefix) once, for caller and server names alike; `eq`, `hash`,
`matches_mailbox` and the encoder then agree by construction and
`mailbox_names_eq` is deleted with its tests.

### D2. Folder role from attributes and names: two tables, already different **[defect-live] (minor) + [shape]**

- `types/mailbox.rs::MailboxInfo::special_use()` - attributes, then name table
  {"all", "all mail", "sent", "sent items", "sent messages", "drafts",
  "draft", "trash", "deleted", "deleted items", "deleted messages", "spam",
  "junk", "junk e-mail", "bulk mail", "archive", "archives", "flagged",
  "starred", "important", "inbox"}; leaf by `rsplit` of a lowercased name.
  Production-dead (only decoder tests call it).
- `account/pim.rs::folder_role` + `folder_role_from_attributes` - attributes
  (no `\All`, `\Flagged`, `\Important`; `\Inbox` via `Custom`), then name table
  {"inbox", "sent", "sent mail", "sent messages", "draft", "drafts",
  "archive", "archives", "trash", "deleted", "deleted messages", "junk",
  "spam", "junk mail"}; leaf via `folder_registry::leaf_name`.

"Sent Items", "Deleted Items" (Exchange) and "Junk E-mail" are recognized by
the dead copy and not by the live one; "Sent Mail" (Gmail) the reverse. The
method on the protocol type is the one the next consumer will reach for.
Owner: one classifier in `types/mailbox.rs` (attributes authoritative, one
name table, namespace-aware - see L1), with `pim` mapping `SpecialUse ->
FolderRole`.

### D3. What a MYRIGHTS set permits **[shape] + [defect-latent]**

- `types/acl.rs`: `can_read` = l+r, `can_insert` = i, `can_delete` = t+e;
  legacy `c` / `d` parsed but never interpreted.
- `account/pim.rs::rights_from_myrights`: `may_remove_items` = t alone (no
  e), `may_rename` / `may_delete` = x or legacy d, `may_create_child` = k or
  legacy c.
- `folder_registry::listing_selectable`: `can_read`.

`can_insert` / `can_delete` are production-dead and disagree with the live
projection (t+e versus t). RFC 4314 Section 2.1.1's legacy expansion lives
only, and partially, in pim. Owner: `MailboxRights::parse` expands legacy
letters per RFC 4314 2.1.1 at parse time and exposes the one projection;
pim delegates.

### D4. Which capability (and ENABLE state) a command needs **[defect-live]**

Answered in the encoder's per-arm `missing(..)` gates
(`encode/dispatch.rs`) and again, separately written, in the connection
handle (`extensions.rs` ACL/METADATA/QUOTA/NAMESPACE/ID/ENABLE/COMPRESS,
`mailbox.rs`, `helpers::require_condstore` / `require_searchres`, `uid_ops`,
`sort_thread`, `auth`). This is not a single answer re-checked at two
chokepoints; the predicate itself is restated and differs:

- SELECT/EXAMINE with QRESYNC parameters: the handle requires QRESYNC to be
  ENABLEd (`validate_qresync_params`, RFC 7162 Section 3.2.3 MUST); the
  encoder, documented as the live authority at the head of the queue, only
  checks `has_capability(QResync)`, i.e. ADVERTISED.
- UID FETCH ... VANISHED: handle requires ENABLEd (`uid_ops`), encoder checks
  advertised. UID FETCH is pipelinable, so a pipelined CHANGEDSINCE+VANISHED
  fetch is gated only by the weaker encoder check.
- AUTHENTICATE PLAIN: `connection/auth.rs` tests
  `capabilities.contains(&Capability::Auth("PLAIN"))` directly, not through
  `supports`.

Owner: a `Command::requirements() -> Requirements { capabilities,
enabled, states, refused_after_selection }` beside `CommandKind::
legal_states`, read by both the handle and the encoder against
`ConnectionCaps` (A10); search criteria contribute through
`SearchKey::required_capabilities()` (A7).

### D5. SASL vocabulary, payloads and policy across IMAP, ManageSieve, SMTP **[defect-live]**

- Mechanism vocabulary: `bifrost-imap::AuthMechanism` (with `Login` meaning
  the IMAP LOGIN command), `bifrost-smtp::Mechanism` (public, `Login` meaning
  SASL LOGIN), and the SCRAM token in `bifrost-sasl`. SMTP's `Mechanism::name`
  derives SCRAM names from `ScramHash::mechanism_name` and documents that as
  the single authority; IMAP's `AuthMechanism::name` hard-codes
  `"SCRAM-SHA-256-PLUS"` etc. One rule, held by one crate and broken by the
  other.
- PLAIN payload built three times: SMTP rejects NUL inside authcid / passwd
  (RFC 4616); `connection/auth.rs::authenticate_plain` does not (zeroized);
  `account/sieve.rs` does not and is not zeroized. A username containing NUL
  re-splits authzid / authcid on IMAP and Sieve.
- XOAUTH2 payload: `bifrost-sasl::xoauth2_payload` strips `\x01` from user and
  token (it has a test for `"user\x01auth=Bearer evil"`). `account/sieve.rs`
  hand-builds `format!("user={identity}\x01auth=Bearer {}\x01\x01")` - the
  injection the sasl builder exists to prevent, and a plaintext `String`
  rather than a `Secret`. The 26aa7c25 consolidation missed this site.
- Policy: IMAP prefers OAUTHBEARER then XOAUTH2 ("mirroring SMTP's
  `OAUTH2_MECHANISMS` order" - agreement by comment); Sieve offers XOAUTH2
  only and PLAIN only (no SCRAM). IMAP's cleartext gate is
  PLAIN/CRAM-MD5/LOGIN for passwords and all OAuth; `AuthPolicy`'s doc lists
  "PLAIN, XOAUTH2, CRAM-MD5, and LOGIN"; Sieve gates every mechanism.
- `types/secret.rs::SecretString` is a near-verbatim copy of
  `bifrost_sasl::Secret` with a hand-rolled constant-time `eq` where the
  original uses `subtle`.

Owner: `bifrost-sasl` gains the mechanism enum (with
`sends_reusable_credentials`, channel-binding, wire name), `plain_payload`
returning `Result<Secret, SaslError>`, the OAuth preference order, and
`Secret` used directly by IMAP; IMAP, Sieve and SMTP select from it.

### D6. Message-ID extraction: two parsers in one hydrated `Message` **[defect-live] (minor)**

`types/envelope.rs::bare_message_id` / `first_in_reply_to` use a structural
scanner that skips `<..>` inside quoted strings and comments and falls back to
a bare token; `bifrost_types::mime::HeaderMap::message_ids` takes the first
`<` and next `>` naively. `account/pim.rs` builds `Message::in_reply_to` with
the first and `Message::references` with the second, so
`"\"<fake>\" <real>"` yields `real` in one field and `fake` in the other; the
other providers use the naive one. Owner: `bifrost-types` mime, promoting the
structural algorithm; the IMAP copy becomes a call.

### D7. What a server event means for sync **[shape]**

Three vocabularies for one stream: `TypedEvent` (connection), `IdleEvent`
(connection/idle) and `EventImpact` (`types/events.rs`, reached only through
the crate-private `ergonomics.rs`). `EventImpact` and
`push::map_idle_event` each decide what an event means and disagree:
`Recent` -> `None` versus invalidate-selected; NOTIFICATIONOVERFLOW ->
`NotifyRegistrationLost` versus coarse `Unknown`. `TypedEvent::MetadataChange`
and `ServerMetadataChange` are never constructed in production but every
mapping handles them. Pick one meaning layer (the account's), delete or
rewire `EventImpact`, and make the TypedEvent -> IdleEvent step lossless
(A15).

### D8. Mailbox display <-> wire conversion under a mode **[shape]**

Decided in `codec/decode/mailbox.rs::decode_mailbox_from_wire`,
`encode_mailbox_str` (with the INBOX-prefix rewrite only here),
`MailboxName::child` (`utf8 ? leaf : encode_utf7(leaf)`),
`MailboxName::parent` and `from_account_id` (`from_utf8_lossy` or
`decode_utf7`), and `MailboxName::identity` (`encode_utf7` for canonical).
Owner: a codec-side `WireMode::{encode, decode}` pair that `MailboxName`
calls; the INBOX rule from D1 lives there or in `MailboxName` construction,
not in the encoder alone.

### D9. Parallel tables beside the dispatcher, kept in step by hand **[shape] + [defect-latent]**

Each of these is a summary of some other code, with nothing forcing agreement:

- `decode/extensions.rs::starts_known_untagged_response` `DIRECT_KEYWORDS`
  versus the `alt` list in `parse_untagged_with_backing`: decides whether a
  parse failure is a connection-fatal violation or a tolerated unknown
  response. A new parser added to the `alt` without the keyword is silently
  tolerant.
- `decode/envelope_fetch.rs::has_closed_grammar` versus the arms of
  `fetch_attr_value_inner`: same strict/tolerant gate for FETCH attributes.
  `MODSEQ` is listed as closed grammar while its arm swallows a malformed
  value anyway.
- `flags_caps::response_code_has_overflow` (code names and widths) versus
  `response_code_inner` arms.
- ResponseCode names: decoder string -> variant (`response_code_inner`),
  `account/error.rs::response_code_name` variant -> string, and
  `imap_response_code` variant -> `bifrost_types::ImapResponseCode`, a
  full mirror enum. Put the wire name on the code kind once.
- `CommandKind::untagged_routing_key` is documented as "a summary of
  `classify`" checked pairwise by a test - the brief's exact anti-pattern.
  Derive it from a classification table.
- `types/profile.rs`: production `rev2_baseline_includes` is a `matches!`
  (non-exhaustive), while the test oracle `capability_matrix::
  in_rev2_baseline` is the exhaustive match that forces a decision for every
  new `Capability`. The guard sits on the copy, not the original: make the
  production list the exhaustive match and delete the oracle copy.
- `classification::classify` ends in `_ => { debug_assert!(false); Impossible
  }`, which defeats exhaustiveness: every row is `(_, UR::X)`-shaped, so
  matching on the response first and the command second would let the
  compiler catch a missing row.

Owner: table-driven decode dispatch (keyword -> parser + strictness + numeric
width) and a classification table from which routing keys are computed.

### D10. Other duplicates noted

- "seq-match-data requires known-uids": handle and encoder (A5).
- "flags writable in STORE/APPEND": encoder and connection (A9).
- Date validation: SEARCH and APPEND (A8).
- UIDVALIDITY zero: six sites (A4).
- Reversed range: seven sites (A1).

---

## 3. Structure

### S1. Dependency edges point upward **[shape]**

- `codec/classification.rs` imports `crate::connection::NotifyFlags`.
- `codec/encode/mod.rs` imports `crate::connection::literals::literal_marker_at`
  (wire grammar living in the connection layer).
- `codec/encode/dispatch.rs` calls `crate::connection::ImapConnection::
  search_criteria_contains_atom` - the encoder reaches into the handle type.
- `types/command.rs` imports `crate::connection::SessionState` for
  `legal_states`.
- `types/events.rs` implements on `crate::TypedEvent` (a connection type).
- `error.rs` (crate root error) embeds `crate::account::sieve::
  SieveResponseCode`.

The protocol core should be a leaf: `types <- codec <- connection <-
account`. Move `SessionState` and the `NotifyFlags` derivation (from
`NotifySetParams`) into `types`, the literal-marker scanner into `codec`, the
search grammar into a `SearchKey` AST in `types`, and give ManageSieve its own
error mapped at the account boundary.

### S2. ManageSieve is a second protocol hidden in `account/` **[shape]**

`account/sieve.rs` (about 1300 lines) is a full RFC 5804 client: its own
response reader, quoting, response-code enum, SASL negotiation and payloads.
It is not account logic and its error variant leaks into the IMAP crate's
root `Error`. It belongs beside `codec`/`connection` as a sibling protocol
module (or its own crate, as SMTP is), consuming `bifrost-sasl` for every
payload (D5), with `account/` only adapting it to the filter lane.

### S3. Crate-root `allow(dead_code)` hides dead copies of live decisions **[shape]**

Dead today: `ImapAtom`, `validated::ObjectId`, `GmailMessageId`,
`GmailThreadId`, `types::Address`, the `EnvelopeAddress -> Address`
conversions, `MailboxInfo::special_use`, `MailboxRights::can_insert` /
`can_delete`, `classification::mailbox_names_eq`, `EventImpact` production
use, `TypedEvent::MetadataChange` / `ServerMetadataChange` construction. Three
of these are second copies of decisions that have a live owner elsewhere
(special_use, can_delete, mailbox_names_eq), each kept green by its own
tests while the live owner differs - the "copy and its test agree
indefinitely" shape. All are crate-private (not published API). Recommend
`#![warn(dead_code)]` on `types/` and `codec/` specifically (small, closed
vocabulary, unlike the measured 105-warning crate-wide case), then wire each
item in where A3 says it belongs, deleting only what has no consumer.

### S4. Command shape doubles on the UID axis **[shape]**

Ten UID/sequence twins (`Search`/`UidSearch`, `Fetch`/`UidFetch`, ...)
double every match in `kind`, `mailbox_target`, the encoder, and the
classifier, while `CommandKind` collapses them, so classification cannot
tell UID FETCH from FETCH (`(CK::Fetch, VANISHED (EARLIER)) => OnlySolicited`
also admits a non-UID FETCH, which cannot solicit VANISHED). Prefer one
variant with `target: Addressed { Uid(UidSet), Seq(SeqNumSet) }` (A1), which
both halves the matches and lets `CommandKind` carry the addressing.

### S5. Command parameters live in `response.rs`; request types duplicated in `sync.rs` **[shape]**

`QresyncParams` and `SelectOptions` (command-side) sit in
`types/response.rs`; `types/sync.rs` defines `SyncSelectOptions` /
`SyncFetchRequest` as "higher-level" duplicates that convert down to them.
One typed request per command (A5), in a `request` module.

### S6. Protocol-value behaviour living in `account/`, account identity living in `types/` **[shape]**

- `CompactUidSet` (folder_registry) is IMAP set algebra (A1).
- `get.rs::merge_fetch_response` encodes FetchResponse merge semantics
  (which items are mutable state, which fill gaps) and is shared with pim;
  it belongs on `FetchResponse`.
- Conversely `MailboxName::account_id` / `from_account_id` /
  `cmp_account_order` (the `~imap-wire:` FolderId encoding) are account
  identity policy embedded in the protocol type; they belong with
  `encode_object_id` in the account layer (or a dedicated identity module),
  which also hosts the single `MessageRef` (A3).

### S7. The crate error is a flat bag with per-variant side tables **[shape]**

`Error` is about 30 variants with a hand-written `PartialEq` (because of
`Arc<io::Error>`), plus `is_connection_fatal`, `attempt`, `with_attempt`,
`response_code` as four more per-variant tables. `attempt:
Option<ImapAttempt>` (a one-field newtype over `TransmissionState`) is
optional on `No` / `Bad` although the docs say a tagged NO/BAD is by
definition acknowledged. A split into `ServerStatus { status: No | Bad | Bye
| AuthRejected, text, code }` (acknowledged by construction), `Transport {
kind, attempt: TransmissionState }`, `LocalRefusal { kind }` (InvalidInput,
InvalidState, MissingCapability(Capability), StateChangedBeforeSend, ...) and
`Internal { mid_exchange: Option<TransmissionState> }` would make
fatality and attempt state functions of the category, not tables to keep in
step.

### S8. Test layout mirrors the pre-split monolith **[shape]**

The decoder was split into nine grammar modules but `codec/decode/tests.rs`
is a single ~14,500-line file and `codec/encode/tests.rs` ~8,400 lines. Tests
should sit with the grammar they pin (or be golden transcript tables per
response family), which also makes the D9 tables testable per row. The
`capability_matrix` oracle (D9) is a test copy of production logic and
should go once production is exhaustive.

---

## Lateral findings

### L1. Role folders ignore namespace; shared folders can win the tie-break **[defect-live, needs confirmation]**

`pim::role_folder_from_entries` filters every registry entry, personal and
shared (`FolderEntry::shared_owner` is not consulted), by `folder_role`, ranks
attribute matches above name matches, and breaks ties with
`MailboxName::cmp_account_order`, i.e. lexicographic display order. On a
server without SPECIAL-USE that exposes shared folders, personal `Trash`
and shared `Shared/alice/Trash` both match by name with equal rank, and
`"Shared/alice/Trash" < "Trash"`, so `delete` / trash fallback and
`role_folder(Sent|Drafts)` can resolve to another user's folder. With the
leaf-name rule for INBOX, `Archive/Inbox` (`'A' < 'I'`) beats `INBOX` for
`quota_probe_folder`, and `containers_list` labels every shared user's
`INBOX` as `FolderRole::Inbox`. Role resolution should be restricted to the
personal namespace (and INBOX matched only as the top-level name).

### L2. Compile-fail doctests that never run and point at non-existent paths **[defect-latent]**

`crates/imap/Cargo.toml` sets `doctest = false`. `MailboxName`'s doc carries
two `compile_fail` blocks asserting there is no `From<String>` /
`From<&str>`, importing `bifrost_imap::MailboxName`, which is not exported;
they never run, and if run would pass for the wrong reason (unresolved
import). The `types/search.rs` module example imports
`bifrost_imap::types::SearchCriteria` (private) and links
`crate::ImapConnection::search` (crate-private). The guarantees these
blocks advertise are unpinned. Either move them to a `tests/` trybuild-style
check against a crate-internal path or drop the claim.

### L3. `MailboxArgument::from(String)` bypasses `MailboxName::new` **[shape]**

`MailboxArgument` converts caller text through `MailboxName::from_decoded`
(no validation) with the comment "Text is validated by the encoder", while
the rest of the crate validates at `MailboxName::new`. Two validation
boundaries for the same input; the encoder's NUL check is the only one
applying on that path.

### L4. `decode_utf7` maps control characters to U+FFFD without signalling **[shape]**

Together with A16 this means a server mailbox name containing a control
byte decodes to a display name that re-encodes differently, which is caught
only because `MailboxName` keeps raw wire bytes; any path that drops the
wire bytes (for example `from_decoded`) silently changes identity.

### L5. `EncodeOptions` clones the capability and enabled vectors per command **[optimization]**

`build_encode_options` copies `capabilities` and `enabled` for every command
encoded; with `ConnectionCaps` (A10) behind an `Arc` this becomes a refcount
bump. Minor, but it is on every command.

### L6. `Capability::Thread` / `Auth` case handling differs from `ServerProfile` **[shape]**

`Capability::from_imap_str` stores `Auth` and `Thread` suffixes UPPERCASED
(from the uppercased token) while `QuotaResource` and `Rights` preserve the
original case; `ServerProfile` then uppercases `Auth` / `Thread` again. One
canonical spelling per variant would remove the second pass.
