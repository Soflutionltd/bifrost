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

**Reported by:** types.

The wildcard mappings are gone: `From<ProtocolKind> for Protocol` and
`From<ProtocolKind> for ProtocolSalt` live in bifrost-types,
`Account::protocol()` is required, and the campaign `protocol` parameter is
documented as the consumer's salt namespace. Residue:

- `recovery::derive_server` decides "is this an SMTP reply" from
  `Cause::Wire(WireCause::Smtp(_))` (`smtp_reply`), not from the error's own
  `protocol` field, so one error answers "which protocol" with two fields.
- Providers stamp `.provider(..)` and `.protocol(..)` at each builder call
  (Gmail about 15 sites, Graph about 14) although the account knows both once;
  an account-owned error context would stamp them at the boundary.
- Collapsing the vocabularies (`ProtocolKind`, `Protocol`, `Provider`,
  `ProtocolSalt`, `PushSource`) removes published items and needs the owner.

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

## CON-052 - Is a Gmail `NotFound` dischargeable under the cursor bridge?

**Reported by:** google.

The two inventory sites now share one evidence-producing helper in
`account/cursor.rs`, and repair reports its evidence. Residue: the inventory
walk still discharges a proven absence silently, because `InventoryEvent` has
no per-object evidence variant, so the engine never sees the proof. That needs
a shared inventory API change in bifrost-types. (Mutation 404 handling stays
separate on purpose: it proves target absence, not inventory coverage.)

