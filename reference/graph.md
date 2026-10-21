# bifrost-graph reference

Generic net translation receives `throttle_scope_for` explicitly for REST and EWS.
Body diagnostics use net's shared byte cap and truncation marker.

The HTTP wire funnel does not receive `AccountOperation`. Net uses its
method default: PATCH and POST do not replay after an in-flight drop, even
when logical recovery later permits retry of an idempotent update. Conditional
PATCH and mixed batch POST need request-specific replay analysis; widening
all updates from their operation label is insufficient. An If-Match write
that committed can reject its replay with a stale etag.

Current architecture of the Graph Account-layer code under
`crates/graph/src/account/`. Public surface:
`bifrost_graph::account::{GraphClient, GraphAccountFactory}` -
`GraphClient` carries credentials / endpoints into the factory, consumers
use the returned `Arc<dyn Account>`. Raw REST helpers and wire types are
crate-private.

The same `Account` impl owns Graph's Stage 1 PIM action surface plus the
Stage 3/4 contact and calendar primitives; unsupported gaps are advertised
through `pim_methods`.

Graph change-tracking is per-collection: message deltas per
`mailFolders/{id}/messages`, event deltas per `calendars/{id}`, contact
deltas per `contactFolders/{id}`. Cursors are scoped to
`CursorScope::FolderType { folder, ty }`, each wraps one `@odata.deltaLink`,
and `inventory_stream` / `changes_stream` walk the `@odata.nextLink` chain
to the final page. Public folders (opt-in) instead poll a watermark cursor
(no delta token). Mutations route through `POST /$batch` with `If-Match`.
Message, conversation, and hydrated container ids from a shared mailbox carry
its owner tag. A thread operation decodes that tag before querying its
members, routes the query through `/users/{owner}`, keeps the returned
message ids qualified for the later batch write, and resolves any folder it
needs (`delete_thread`'s Trash) in that same mailbox. The tag on thread ids
arrived with cursor envelope v2, which forces a reseed.

## Module layout

`crates/graph/src/account/`:

- `mod.rs` - `GraphAccount`, `GraphAccountFactory`, `PushMode`,
  the `impl Account` surface.
- `capabilities.rs` - `build_capabilities` per `PushMode`.
- `cursor.rs` - `GraphCursorKind`, `GraphCursorPayload`,
  `GraphPageMarker`, envelope encode/decode, scope/kind mapping.
- `scopes.rs` - `discover_cursor_scope_events`,
  `discover_membership_events`, `scope_lifecycle_events`,
  `CursorIndex`, `FolderTree`.
- `foreign.rs` - foreign (shared/delegate) mailbox folder codec:
  `encode_foreign` / `parse_folder` / `ParsedFolder` / `owner_tag`.

Crate root: `paging.rs` - `PageWalk`, the shared repeated-link and page-budget
bound used by Graph collection traversals, plus the search cursor codec that
resumes within an over-delivered page (see "Bounded `nextLink` traversal").
- `inventory.rs` - initial delta walk with per-type projections, page
  pagination, inventory entry projection from Graph JSON.
- `changes.rs` - delta-token-driven change stream over the
  cached `@odata.deltaLink`.
- `get.rs` - `$batch`-backed hydration with per-projection
  `$select` lists.
- `batch_routing.rs` - `partition_routable`, the pure per-item split every
  `$batch` chunk builder runs before it sends: ids whose subrequest was
  built, and ids whose subrequest could not be built at all (a stale
  shared-mailbox owner). The rejected half goes to the caller's failed lane
  and the chunk proceeds; an empty routable half skips the POST. Generic
  over the per-site URL builder, so the lane rule is pinned once while each
  site keeps its own classification.
- `push/` - the push subscription surface, split by arm. `mod.rs` declares the
  modules and re-exports the `pub(crate)` doors the rest of `account/` calls
  (`push_subscribe` / `push_unsubscribe`, `retire_all_graph_subscriptions`,
  `PushEndpoint`, `GraphSubscriptionGroup`, `EwsSubscriptionState` /
  `EwsSubscriptionScope`); everything else is `pub(super)` inside `push`.
  - `push/dispatch.rs` - the two `Account` doors: the empty-scope refusal, the
    per-scope poll-only split, the `PushMode` dispatch (`subscribe_eligible`),
    and the positional three-lane outcome ledger (`push_item_ids` /
    `finalize_push_outcomes`). The dispatcher builds the ledger; each arm
    closes it itself, before it registers anything.
  - `push/common.rs` - what both arms share: `PushEndpoint`, the two
    `Unsupported(PushSubscribe)` refusals (whole-request and scope-correlated),
    the shared `account_closed_error`, and the handle codec
    (`graph_handle` / `ews_handle` / `decode_handle`, `new_handle_token`).
  - `push/webhook.rs` - the Graph `/subscriptions` arm: `resource_for_scope`,
    `subscribe_graph` with its create rollback, the per-handle
    `GraphSubscriptionGroup` / `GraphSubscriptionState`, and teardown -
    `unsubscribe_graph`, `close()`'s `retire_all_graph_subscriptions`, and the
    `mark_group_tearing_down` / `remove_subscription_from_groups` pair that
    makes it race-free against renewal.
  - `push/renewal.rs` - the renewal health worker that re-issues expiring
    subscriptions, recreates a vanished one (`replace_gone_subscription`, which
    hands `create_and_install_replacement` to a spawned task, and
    `install_replacement`), emits Disconnected/Reconnected/Terminated on
    renewal failure, keeps a terminally failed row for teardown, and retires
    its own worker slot.
  - `push/ews.rs` - the EWS streaming arm: `ews_subscribable_folder_id`, the
    `restId` -> `ewsId` translation (`translation_input_chunks` /
    `reconcile_translated_ews_scopes`), and the `EwsSubscriptionState`
    registration plus topology bump the worker reads. Like the webhook arm it
    refuses a closed account (`shutdown` cancelled, which only `close()` does)
    before the translation request, and re-checks under the registration write
    lock for a `close()` that began mid-translation; no worker is spawned for a
    dead account.
  - `push/tests/` - the push suite, split along the same seams: `fixtures.rs`
    (the subscription-row, scope, translation-input and ledger builders more
    than one arm needs), `webhook.rs`, `renewal.rs`, `ews.rs`, and
    `dispatch.rs` for the mode-agnostic contract - the per-scope lane rules and
    the whole-request refusals, which run in both push modes and belong to
    neither arm.
- `push_stream.rs` - the broadcast-backed `push_stream` adapter, selecting the
  receiver against the shutdown token and a watch latch for terminal health,
  plus `publish_terminal` and `ensure_ews_worker`, the spawn-on-demand helper
  `subscribe_ews` calls.
- `ews_stream.rs` - EWS Streaming Notifications fallback:
  Subscribe / GetStreamingEvents / Unsubscribe XML, scope
  recovery, the long-lived worker loop - generic over the crate-private
  `EwsExecute` transport seam (`EwsClient` in production, a scripted
  double in tests). One-shot SOAP calls use the buffered `execute` funnel;
  `GetStreamingEvents` uses its separate chunked response seam and frames
  each complete response message before parsing, so notifications arrive
  while Exchange keeps the outer SOAP response open. Framing keys on the
  element's LOCAL name, never on a serialized `m:` prefix - a namespace
  prefix is a writer-chosen alias, and matching the bytes discarded every
  otherwise-valid response without reporting a failure. A framed response
  message keeps its typed `EwsError`: a classified response error inside
  the stream (`ErrorAccessDenied` and friends) exits through
  `WatchEvent::Terminated`, and only a malformed or truncated frame
  reconnects. At `push_subscribe` (in `push/ews.rs`),
  Graph REST folder `restId`s are translated once through
  `/me/translateExchangeIds` - deduplicated and chunked to Graph's 1,000-id
  request cap - to `ewsId`s and retained beside their `CursorScope` in
  subscription state: the SOAP Subscribe body uses those EWS ids and
  notification routing maps the returned EWS parent id back to every original
  scope without a second request. The worker deduplicates equal `ewsId`s for
  the Subscribe body while preserving every DISTINCT matching scope for
  invalidation.
- `public_folder.rs` - no-delta-token public-folder sync: the
  watermark + throttled deletion-scan poll, inventory/changes streams,
  `EwsItem` -> entry/change projectors, hierarchy discovery driver.
- `autodiscover.rs` - Exchange Autodiscover: `GetUserSettings`
  (public-folder routing, content-mailbox SMTP) and the POX
  `alternativeMailboxes` delegate lookup, wired into `open` via
  `with_delegate_discovery()`, over `AccountNet`. Both lookups follow in-body
  redirects under one rule set (see the Autodiscover paragraphs under "Which
  URLs may carry the bearer").

`crates/graph/src/ews/`: `client.rs`
(`EwsClient::execute(body, &EwsHeaders)`), `ops.rs` (`find_folder` /
`get_folder` / `find_items` / `get_item` + pure body builders),
`parse.rs` (quick-xml parsers returning `EwsError`, `decode_replica_list`),
`mod.rs` (`EwsError`, `SoapFaultCode`, `EwsHeaders` routing pair).
- `mutate.rs` - `bulk_set_flags` / `bulk_move` / `bulk_destroy` over
  `$batch` with `If-Match` and a `Retry-After`-aware throttle path.
- `pim/` - Stage 1 PIM primitives and Graph conveniences, split by topic.
  `mod.rs` declares the modules and re-exports the `pub(crate)` doors the
  rest of `account/` calls; everything else is `pub(super)` inside `pim`.
  - `pim/messages.rs` - per-message writes (add-to-container, category,
    extended property, read flag, importance) and the shared `$batch`
    write pipeline: `resolve_target_ids` / `resolve_target_values`,
    `message_batch_url`, `patch_messages` / `move_messages`,
    `submit_write_batch(_with_targets)`, etag caching.
  - `pim/send.rs` - `send_message` / `send_raw_message`, send-as stamping,
    the deferred-send-time property, and the scheduled-send handle codec
    with `cancel_scheduled_send` / `reschedule_send`.
  - `pim/drafts.rs` - draft create/update/discard/send plus the
    `DraftPatch` and `SendRequest` to Graph message-body projections.
  - `pim/search.rs` - `search` / `search_messages`, the KQL and OData
    filter builders, the shared-mailbox walk, and the versioned opaque
    search cursor (`SearchCursor`, `encode_/decode_search_cursor`).
  - `pim/containers.rs` - container list and CRUD across primary, shared
    and public namespaces, well-known folder roles, `container_from_folder`,
    and `trash_container_id` with its per-mailbox cache plus
    `container_is_trash`.
  - `pim/identities.rs` - identity snapshot and vacation (automatic
    replies) get/set with their Graph datetime projections.
  - `pim/threads.rs` - the thread doors `thread_hydrate` / `move_thread` /
    `delete_thread` and their owner routing.
  - `pim/hydrate.rs` - typed hydration: `message_hydrate`,
    `ews_read_folder`, the public-folder EWS read arm, and the Graph-JSON
    and EWS-item projections onto `Message`.
  - `pim/common.rs` - the few helpers two or more of the above share:
    `object_id_from_value`, `pim_protocol_error`,
    `graph_extended_property_id`, and the Graph time formats.
  - `pim/tests.rs` - the PIM suite, kept as one module across the split.
- `filters.rs` - Stage 2 Inbox `messageRules` typed-rule
  list/create/update/delete plus local validation.
- `blob.rs` - `open_blob` / `open_blob_range` over Graph
  attachments (`/messages/{id}/attachments/{aid}/$value`),
  including the reference-attachment short-circuit, plus
  `open_raw_rfc822` (whole message via `/messages/{id}/$value`).
- `cloud.rs` - `host_attachment`: OneDrive resumable upload + `createLink` in one
  call. Session/link POSTs go through `GraphClient::post` (conflict `rename`); the
  pre-authed chunk PUTs and the session cancel go through `execute_aux` with
  `AuxTarget::Anonymous` (no bearer, redirects disabled per request) and the
  upload resumes on `202 Accepted`. The session's `uploadUrl` is admitted at
  receipt, before any PUT, by the rule that lives in `origin.rs` as
  `Base::admit_upload` (reusing the shared `validate` shape rule), yielding an
  `UploadUrl` that is the only thing the anonymous send path accepts. The rule:
  an absolute `https` URL on any host without userinfo, or plain `http` only on
  the configured Graph origin (which already holds the account bearer). This
  module's `admit_upload_url` only classifies a refusal: anything else is
  `GraphError::ContractViolation` -> `Protocol(ContractViolation)`,
  and the refusal never quotes the URL, since the URL is itself the session
  credential. Same-origin admission cannot apply (session URLs live on
  SharePoint and OneDrive hosts, never the Graph host) and there is no host
  allowlist: Microsoft documents `uploadUrl` as opaque, and the hosts vary by
  tenant, product and national cloud. Redirects are disabled because the
  transport's redirect walk cannot apply the upload rule to a hop, so following
  one would send the chunk, and a URL that is itself a credential, somewhere
  never admitted; Graph's upload flow does not redirect, so a 3xx on a chunk
  PUT is `Protocol(ContractViolation)` naming the status and never the
  `Location`. A failed upload deletes its session on its own bounded budget
  (`SESSION_CANCEL_TIMEOUT`), separate from the upload budget because the upload
  may have failed exactly because that budget ran out: one anonymous `DELETE`
  on the admitted URL, never replacing the upload's own error, skipped once the
  account has shut down and abandoned if shutdown fires mid-cancel, and not
  spawned, so a caller who drops the hosting future gets no cancel and the
  session expires on OneDrive's clock. A 202 must carry a valid resume offset
  (`admitted_resume_offset`): an unparseable body, a missing
  `nextExpectedRanges`, or a first entry that is not a byte range is
  `GraphError::Json` -> `Protocol(ParseFailed)`; an empty range list, an
  offset that does not advance, one past the end of the bytes just sent, or
  one that reaches the total (every byte acknowledged without the 200/201
  drive item) is `Protocol(ContractViolation)`. No offset is ever guessed,
  because resuming at the chunk end skips bytes the server did not accept. An
  offset short of the chunk end is legitimate on any chunk, the final one
  included. The whole upload has one budget
  (`UPLOAD_TOTAL_TIMEOUT`), carried INTO each chunk PUT as the remainder of
  its bifrost-net total deadline, so an expiry is classified at the stage it
  hit (`Transport(Timeout)`, `Unsent` before dispatch or between chunks,
  `InFlight` awaiting headers); a timer around the loop threw that stage away
  and reported the expiry as a provider parse failure. A shutdown between
  chunks is `Unsent` (no file exists until the final chunk is accepted), one
  that cuts a PUT short is conservatively `InFlight`, so this non-idempotent
  upload reconciles rather than replaying into a second file. `ShareScope`:
  `Anyone -> anonymous`, `Organization -> organization`.
- `error.rs` - blob-not-byte-stream warning helper. Classification
  helpers live in `graph_error.rs`.

`GraphClient` owns the REST test seam. Every REST helper - raw MIME import
included - funnels through one private `execute_wire`, which adapts the
production `bifrost_net::Response` into a Graph-local wire response. JSON
bodies are serialized by the funnel rather than by `RequestBuilder::json`
(identical bytes and content type, plus the same `EncodeBody` failure) so the
funnel is non-generic and the one non-JSON body can share it; `post_mime`
previously built its own request on `AccountNet` and was the one wire path the
seam could not see.

The funnel sends `Content-Type` only when a request has a body: JSON writes use
`application/json`, and raw MIME import uses `text/plain`. Bodyless GET, DELETE,
and POST requests carry no content type.

Responses are scripted at the WIRE, not at the funnel.
`script_rest` / `script_aux` install a `bifrost_net::test_support::
ScriptedDispatch` and bind an `AccountNet` to it, so a scripted status travels
the production retry loop, rate-limit permit, redirect walk, and bandwidth
meter before the funnel sees anything. Only the request RECORDING stays local
(`record_wire` / `record_aux`), because this crate's recorded shape is richer
than the transport's `RequestSnapshot`: the JSON body arrives parsed and
`If-Match` / `Prefer` are lifted out of the header bag.

This replaced a Graph-local response queue that restated bifrost-net's status
contract in an `into_net_outcome` helper. That restatement is what xc-3 was
filed against: a copy of a contract can agree with itself while disagreeing
with the transport, and this one did - it answered a 401 with `AuthLost`
directly, hiding the fact that bifrost-net forces a token refresh and reissues
the request on a budget separate from `max_attempts` first. A Graph path
meeting a transient 401 recovers without surfacing an error at all.

Consequences worth knowing when writing a test:

- REST and aux share ONE dispatcher, because in production they share one
  wire. A test scripting both lists its responses in wire order, and that
  ordering is itself an assertion (see `hosting_an_attachment_uploads_then_
  mints_a_link`, whose order is session, chunk, link).
- The default policy is `RetryPolicy::disabled()`, so one scripted response
  answers one request. `script_rest_with_retries` opts into the production
  policy for a test that wants to pin the retry loop itself;
  `wire_attempts()` reports the transport's own request count, which is where
  a retried attempt is visible - it is not a second funnel call.
- A script that runs out panics inside bifrost-net's dispatcher rather than
  reaching the network. Same guarantee as before, now enforced once for every
  crate riding the transport instead of per crate.

The script is shared with every client derived from the one a test holds
(`for_shared_mailbox`, `with_outlook_base`), the way `AccountNet` already is, so a foreign-mailbox request - issued by a client
minted inside `GraphAccount::new` - is scripted and recorded alongside the
primary's. That sharing silently weakens any test written past it: a derived
client answers from the primary's queue and records into the primary's log,
so the seam alone cannot say WHICH client issued a request. Asserting the URL
does not recover it either on the delta paths - `initial_delta_url` builds
its `/users/{mailbox}` prefix off `client_for_scope` independently of which
client then sends it, and a `nextLink` / `deltaLink` is whatever Graph
minted - so a walk that fell back to the primary would produce byte-identical
requests. (The per-message write paths do not have that problem: there the
URL is built from the selected client's own prefix, so the URL IS the routing
evidence.) Where the distinction matters, a test roots the shared client in
its own `GraphClient` (`new_for_tests_with_shared_clients`) and arms the
primary with an EMPTY script, so a fallback hits the exhaustion panic instead
of passing quietly - the pattern the foreign delta walks, the thread-keyed
doors, and the mailbox-aware search cursor tests all use. When a test's
account-wide request legitimately goes to the primary (`delete_thread`'s
`/$batch` POST), the primary is armed with EXACTLY that one response, so a
misrouted lookup consumes it and the next primary request panics. The same
applies to every `$batch` POST: `message_reactions` and `get_stream` hydration
always post through the primary client, correctly, because `/$batch` is
account-global on Graph, while their subrequest URLs carry the `/users/{owner}`
prefixes. On those surfaces the routing evidence is subrequest-level only, so
a test asserting owner routing has to read the batch body, not the client the
POST went to.

Two smaller funnels sit beside the REST one, for the wire paths that are not
Graph REST JSON calls. Both now share the REST dispatcher:

- `GraphClient::download_stream` - blob and raw-RFC822 byte streams. Tests
  script the chunk sequence (`ScriptedDownload::Chunks`, framed individually
  by `Canned::Stream` so "the blob stream forwards every transport chunk"
  is checkable), a ranged read (`PartialChunks`, a 206 plus the
  `Content-Range` it answers with), an open failure (`FailedStatus`, which is
  how a 405 "not a byte stream" and every 4xx arrive: bifrost-net resolves the
  status before yielding a body), or a mid-body failure (`ChunksThenError`,
  the only failure the status check cannot pre-empt, surfacing as
  `Error::Network`). The recorded request carries the URL and the caller's
  `ByteRange`.

  `PartialChunks` exists because bifrost-net refuses a ranged read that does
  not return 206 with a `Content-Range` matching the requested window. The
  old Graph-local queue handed back chunks regardless of range, so a ranged
  test could record a `ByteRange` the account never actually put on the wire.
  The scripted header is now checked against the real request.
- `GraphClient::execute_aux` - the pre-authenticated OneDrive chunk PUTs and
  session cancel `DELETE`, and the Autodiscover POST. These are explicitly
  `Unmetered`; bearer handling differs from `execute_wire`. The recorded
  request keeps the header list verbatim (`Content-Range`, `SOAPAction`) plus
  whether a bearer was attached, so "the pre-authed session URL never carries
  the Graph token" is an assertion rather than a comment. Whether a bearer is
  attached is decided by the `AuxTarget` variant, not a flag: only an
  admitted URL can carry it (see "Which URLs may carry the bearer").

The REST, aux, and download surfaces no longer have the graph-T1 limitation:
because they script at the wire, retry, backoff, the rate-limit permit, the
redirect walk, and the ranged-read contract all run below the script and are
observable (`a_transient_5xx_is_retried_below_the_graph_funnel` pins one
funnel call against two wire attempts).

EWS keeps its own `EwsExecute` seam, which answers at the EWS funnel and so
still sits above the transport - the one place graph-T1's gap remains.
`EwsClient::execute` does post through `AccountNet`, so it COULD be scripted
at the wire; it is not, because the trait double replaced three failed
review-only rounds and immediately caught four defects, and the marginal gain
(observing retry on SOAP posts) does not justify rebuilding a working seam.

Calendar/contact primitives live in `calendar.rs` and `contacts.rs`. Graph
calendar `color` is a provider token (not projected); reads request `Prefer:
outlook.timezone="UTC"`. Inbound, Graph's daily/weekly/monthly/yearly
patterns map to an RRULE and an unrecognised inbound pattern shape is omitted;
a weekly pattern with an interval above one also carries `WKST` from
`firstDayOfWeek` (Graph's default Sunday when absent, RFC 5545's Monday
implied when it is Monday). Outbound recurrence is REFUSED, never degraded,
on both create and patch, because a dropped rule writes a one-off and a
dropped part writes a different series than the one asked for. An RRULE that
is valid RFC 5545 but that Graph's `patternedRecurrence` (or this mapping)
cannot express is `Unsupported`: sub-daily frequencies, a `BYMONTHDAY` counted
from the month end, several ordinal `BYDAY` values, a `BYSETPOS` outside 1 to 4
or -1, a part Graph would ignore for the chosen pattern type (`BYDAY` on an
absolute pattern, `BYMONTH` on anything but yearly, `BYSETPOS` on an absolute
pattern), and the RRULE parts with no Graph field at all. A value that is not
an RRULE at all (no `FREQ`, an unknown frequency, an unknown, repeated or
`NAME=VALUE`-less part, both `COUNT` and `UNTIL`, a non-weekday `WKST`) is `Request(Malformed)` naming `recurrence.rrule`; a start
that does not begin with a date is `Request(Malformed)` naming `start`.
`RDATE` and `EXDATE` are refused `Unsupported`: Graph has no carrier for extra
occurrences, and an excluded occurrence exists only as a deletion after the
series is written, which one create or patch cannot do. An absolute series
that reaches a month lacking its day (the 31st every month from January,
February 29th every year), whether the day is explicit or taken from the
start, is `Unsupported`: RFC 5545 skips a date the month does not have, while
Graph moves the occurrence to the month's last day, so writing it would add
occurrences. Only the months the series actually visits within its `COUNT` or
`UNTIL` count, so the 31st every sixth month from July is written as is. Equivalent shapes are
rewritten rather than refused: `DAILY` with `BYDAY` at interval 1, and every
such weekday of each month or year, become weekly patterns; a lone ordinal
weekday (`2MO`, `-1FR`) becomes a relative pattern's index; values RFC 5545
takes from `DTSTART` (the weekday, the day of month, the month) come from the
event start; a UTC `UNTIL` becomes the local end date in the event's zone
(kept on its UTC date when the zone cannot be resolved); and `WKST` is carried
as `firstDayOfWeek` on weekly patterns. The range `startDate` is the series'
local start date. A recurrence patch with no `start` anchors on the series' own
range start, read from the event fetched for the update; on an event that does
not recur yet it is `Unsupported`, since its UTC-normalised start can sit on a
different day than the local one Graph wants and an empty `startDate` is never
sent. A patch that moves `start` on a recurring event without restating the
recurrence re-sends the event's own recurrence with the range start moved along;
the moved start is read by the same rule as an anchor, so one that does not
begin with a date is `Request(Malformed)` naming `start`.
A patch recurrence carrying no rule and no dates clears the series
(`"recurrence": null`). An empty attendee list in a patch is sent as `[]` and
clears the attendees, where omitting it had left them in place. Outbound times map a conservative IANA
-> Windows table, refused pre-payload by what the refusal is about: a
slash-shaped (IANA) id missing from the table may be a real zone this crate
has no Windows name for, so it is `Unsupported`; a slash-free multi-word
value passes through as a Windows id, and anything else that is neither
(`"foo"`, `""`) is `Request(Malformed)` naming its field (`start.timezone` /
`end.timezone`). The read projection
yields an EMPTY `start` for "Graph did not say" (a delta tombstone with no
`start`, or a null `dateTime`), and a read-modify-write consumer echoes
it back; unguarded it reached the wire as `dateTime: ""` under a defaulted UTC
zone. A missing or valueless `end` projects as `CalendarEvent.end` `None`,
which echoes into a patch as no end change, but a caller may still pass an
empty end explicitly. `reject_unusable_write_times` runs first in `create` and `update` (before
the timezone checks, the fetch and the calendar-move refusal): an empty or
whitespace `start` is `Request(Malformed)` naming `start` on both, and an empty
`end` on a create is `Unsupported` (no end is invented). An empty `end` on a
patch is not refused: `graph_event_from_patch` drops it, before it can feed the
all-day inference or `isAllDay`, so an echoed event leaves the stored end
alone. An `EventId` without
the calendar separator was never minted here, so `split_event_id` refuses it
as `Request(Malformed)` with field `event`. A server-derived organizer, a
non-`Confirmed` status, and an RSVP value Graph has no action for stay
`Unsupported`. `responseStatus` maps to
`self_response`; RSVP uses native `accept`/`decline`/`tentativelyAccept`.
Contact and calendar patches accept `expected_version: Option<ServerVersion>`.
An `ETag` is sent verbatim as `If-Match`; a 412 is `ConcurrencyConflict`.
A versioned contact update no longer fetches a validator. Contact updates
refuse an `address_book_id` change as `Unsupported(ContactUpdate)` before I/O:
this Graph path does not issue a folder-aware move, so it does not acknowledge
a request to move a contact. Calendar update still reads series content for
recurrence serialization, but never substitutes that read's change key for the
caller token. Its preflight read failure is attributed to `EventUpdate`, since
the read is part of the update operation. An absent version retains the existing
immediate-read guard. Graph accepts HTTP entity-tag syntax and forwards the
accepted token verbatim; the weak-tag prefix is uppercase `W/`, so lowercase
`w/` is refused as an invalid token.
Delete still has no caller-version input and retains its immediate-read guard.
Other token kinds are refused as `Unsupported` before I/O.
Updates send sparse PATCH (scalar clears as null;
contacts emit `null`/`[]` for emptied buckets). Event organizer/status are
server-derived. Event search uses the Graph Search API for
unscoped non-empty default-mailbox searches, else local; composite `EventId`s
embed the calendar (`{calendar}::{event}`), Search hits use the `$mailbox`
sentinel routed through `/me/events/{id}`. `EventSearchRequest::include_cancelled`
(default `false`) is honoured client-side on both lanes off the selected
`isCancelled`; the Search API cursor advances by the RAW hit count so dropped
hits are not re-read, and `estimated_total` is `None` on both lanes so the
filter cannot over-count. Contact search uses exact email
`$filter` for email-shaped queries, else local. `ContactEmail.kind` maps
to and from Graph `emailAddress.name`, which is a DISPLAY NAME, not a type
label - Graph's contact schema has no per-address type. The round trip is
lossless and consistent, but a consumer must not read a Graph contact's
`kind` as "work"/"home", and a `kind` it writes appears as the address
display name in Outlook.

### Public calendars (EWS)

`calendar_public.rs` mirrors the public contact routing pattern. After the
REST walk, `calendars_list` appends discovery-seeded readable public folders
with `IPF.Appointment` or a dot-separated subclass. Pins and background sync
selection do not restrict listing. Public calendars have every event write
flag false. `foreign.rs` tags collections as `public-calendar\u{1e}<folder>`
and events as `public-calendar\u{1e}<folder>\u{1e}<item>`; native ids and
provenance retain the bare EWS ids. Every typed calendar door dispatches
before REST id/cursor decoding. Missing public routing refuses locally as
`ConcurrencyConflict` with `Unsent` evidence, never a REST fallback.

`events_in_range` uses `FindItem` with `CalendarView`, never an indexed item
view. Exchange expands occurrences and exceptions and hides recurring masters;
a master in the response is a contract violation. Each returned instance
retains its own native ItemId, which `event_get` sends directly to `GetItem`.
The identifier includes occurrence data, rather than being the master's id:
see Microsoft's [identifier format](https://learn.microsoft.com/en-us/openspecs/exchange_server_protocols/ms-oxcext/087b58b5-6e78-45ec-acd3-7dbac7346aed)
and [recurrence expansion](https://learn.microsoft.com/en-us/exchange/client-developer/exchange-web-services/recurrence-patterns-and-ews).
Hermetic SOAP tests pin distinct occurrence and exception ids through both
doors; they do not claim a live-server round trip.

CalendarView has no offset. A non-final response or one reaching the requested
250-row cap is discarded and its time window bisected, left child first.
Both children share the split boundary. The versioned JSON cursor binds the
native folder, original start/end strings and zones, pending windows, accepted
instance ids awaiting delivery, seen ids and cumulative request count. Stable
native instance ids deduplicate events spanning multiple windows. Accepted
ids drain in caller-sized pages without repeating FindItem, and are hydrated
through GetItem for full body and attendee data. Hydration batches 20 ids per
request and runs at most four chunks concurrently, retaining submitted order.
This is a local fan-out bound, not a claim that EWS shares Graph's per-mailbox
REST throttle policy. `EwsClient::get_calendar_items` uses a separate typed
multi-item funnel: HTTP/transport errors, SOAP Faults, malformed documents and
response-count mismatches abort the request. A sole error response to a
multi-id request is a general batch failure and keeps its EWS classification. Application ResponseMessage
errors and per-item kind, identity or projection failures instead name that
item's native id in `Page::failed_ids`; those rows are consumed and the cursor
advances. A deletion from a saved ready list cannot pin that continuation.
Each successful response still verifies its identity and CalendarItem kind. An empty accepted child may
return an empty page with a continuation; only absence of both pending windows
and pending ids means exhaustion. No truncated view is reported as exhausted.

Boundaries use integer 100ns ticks, preserving Exchange date-time precision;
inputs finer than that are refused as malformed. Offset timestamps and local
date-times with a resolvable IANA or Windows zone are accepted through the
same `resolve_time_zone` helper used by the all-day projection. A still-capped indivisible
window fails as `Internal(LimitExceeded)`. The same classification bounds the
whole continuation to 10,000 FindItem requests and 100,000 seen instance ids.
Those are explicit client limits, never a successful truncated result. Like
other provider pages, this is a live view, not a snapshot across mutations.

`ews/calendar.rs` requests calendar properties valid for CalendarItem; no
message-specific fields are sent. GetItem verifies the native identity,
CalendarItem wire kind and `IPM.Appointment` class/subclass. A non-calendar
result is `Unsupported(EventGet)`. Its errors carry `ErrorScope::Event` and
the tagged event id, so `ErrorItemNotFound` becomes `NotFound(Event)` with that
id. Event scope/resource are also preserved by shared transport error mapping
and the sync ledger's version-5 error digest (versions 3 and 4 remain readable).

Projection includes subject, start/end and original timezone ids, exclusive
local all-day dates, location, organizer, required/optional/resource attendees
and response status, text body with preview fallback, UID/change key,
cancellation, sensitivity and free/busy status. Occurrence and exception
`OriginalStart` is preserved in `EventRecurrence::recurrence_id`; a missing
original start refuses parsing rather than inventing a different identity.
Expanded instances do not carry a master RRULE. All-day UTC boundaries are
converted into the supplied item zone before taking their dates; an unresolved
all-day zone is refused rather than shifted silently.

Create, update, delete and RSVP refuse `Unsupported(operation)` before any
request for a tagged public id, including a REST event patch targeting a
public calendar.

Public scoped event search uses indexed EWS FindItem with an escaped OR
Restriction of substring/ignore-case Contains clauses over subject, location
and body. An empty query enumerates the folder. `EventSearchRequest` has no
range; as on the scoped REST `/events` path, search returns stored single
items and recurring masters, without occurrence expansion. Masters project
their EWS recurrence pattern/range through the existing Graph-to-RRULE mapping;
occurrences and exceptions remain available through the range door. The
GetItem shape requests recurrence alongside body and attendee properties.

Search hydrates through the same batched per-item funnel, applies the existing
local text matcher to complete bodies and honors `include_cancelled`.
Only matched, admitted events count toward the result limit. The opaque
cursor binds folder, exact query and cancellation policy, and retains pending
ids, next indexed offset and a cumulative 10,000-request budget. It consumes
failed and filtered rows while progressing to a strictly advancing server
offset. A missing/stalled offset is `Protocol(ContractViolation)`, never
exhaustion. REST and unscoped search behavior is unchanged.

### Public contact books (EWS)

`contacts/public.rs` implements the read-only public contact leg. After its
REST contact-folder walk, `address_books_list` appends discovery-seeded,
readable public folders whose `FolderClass` is `IPF.Contact` or a
dot-separated subclass. `HierarchyOnly` and pins govern background sync,
not this discoverability. Every public book has all three write flags false;
account-wide contact write capabilities still describe the writable REST books.

`foreign.rs` tags public contact collection ids as
`public-contacts\u{1e}<folder>` and item ids as
`public-contacts\u{1e}<folder>\u{1e}<item>`. The RS delimiter cannot occur in
native EWS ids. Dispatch uses the tag before REST cursor decoding or route
construction, independently of routing-map membership. Native ids and
`ContactProvenance` remain bare EWS ids. A recognized public read with no
routing entry refuses locally as `ConcurrencyConflict` with `Unsent`
evidence (refresh local state), including after reopen before discovery.
No public contact id falls back to REST.

`contacts_list` requests contact properties over EWS `FindItem`, using the
folder's content-mailbox headers. The provider-owned JSON cursor contains a
versioned public-contact discriminator, native folder id and indexed offset.
It is bound to that folder and, for search, the exact query. Search cursors
also retain the consumed wire-row count when the caller's limit stops inside
a page; resuming re-reads that page before advancing the server offset. A
non-final response must supply a strictly advancing `IndexedPagingOffset`, including an empty projected page; missing
or stalled offsets are `Protocol(ContractViolation)`, never exhaustion.
The offset counts wire rows, including skipped distribution lists. Estimated
totals are omitted because EWS counts those rows too.

`ews/contacts.rs` supplies the Contact shape and XML field parser. Cards
project display name, email and phone dictionaries (preserving native keys
as labels), company/job title and physical addresses. Email dictionary entries
retain `RoutingType`: only SMTP (case-insensitive) is emitted. When the
attribute is absent, a bare address must parse with `email_address` (the same
parser used by the SMTP crate), with display-name syntax disabled. Explicit
EX, X400, unknown or empty routing types are omitted even when their values
look like SMTP addresses. Omissions emit a counted `skipped_email_addresses`
warning per card; logs contain no address values. Given name plus surname
is the display-name fallback: `ContactCard` has no structured name fields.
`FindItem` does not request Body; notes are projected if supplied, while
`contact_get` explicitly requests the text Body through EWS `GetItem`.
Photos are not projected. `GetItem` verifies the returned native identity,
Contact wire kind, and `IPM.Contact` class or dot-separated subclass before
projecting. Non-contact results, including distribution lists, refuse as
`Unsupported(ContactGet)`. All EWS GetItem failures carry
`ErrorScope::Contact` with the tagged contact id. EWS `ErrorItemNotFound`
uses the scope's resource kind and id, yielding `NotFound(Contact)` here;
existing message hydration/blob calls retain `NotFound(Message)`, as do
unscoped streaming and cursor-scoped folder walks through their historical
Message fallback.

`ContactCard` has no group representation. Distribution lists are skipped
by listing with a counted `skipped_dist_lists` tracing warning per page;
no individual-person card is invented for a group. Public scoped
`contact_search` uses the same EWS FindItem projection, routing, read-only
policy and indexed paging as listing. A non-empty query adds an OR
Restriction of `Contains` clauses with `Substring` / `IgnoreCase` over
display name, given name, surname and indexed email addresses 1-3. The XML
constant is escaped; an empty query enumerates the book without a
Restriction. Result limits count projected contacts, not skipped groups.
Unscoped and REST-scoped searches retain their existing behavior. Create,
update and delete refuse `Unsupported(operation)` before any request for public ids,
including an update targeting a public address book through its patch.
The account-wide write capability flags remain available for personal books.

## `GraphAccount` / `GraphAccountFactory` shape and lifecycle

The `account` module path is public (consumers construct the factory
through it); helper modules and `GraphAccount` stay crate-private.
`GraphClient` is public only as factory input. `new` / `with_api_base` take a
raw token; `with_source` / `with_account_net` take a shared
`Arc<dyn TokenSource>` that `attach_account` hands to bifrost-net. There is
one api-base: the client speaks `v1.0` only, and a beta endpoint is not
configurable because no call site reads one.

`attach_account` is called again on every reopen with the same engine
id. When the client owns a parent `Net` it mints a fresh `AccountNet`,
installs it, and then calls `detach()` on the handle it displaced.
`Net::attach_account` issues a distinct registration token per call and
does not unregister a previous attachment for the same id, so without
that explicit teardown each reopen would leak one meter attachment and
one governor attach count and the host bucket would never be reclaimed.
Detaching after the install keeps the shared counts from reaching zero,
so requests still in flight on the displaced handle continue to meter
against the same counters. When the client was built with
`with_account_net` there is no parent `Net` and the existing handle is
retagged instead, which moves its token rather than minting a new one.

`GraphAccountFactory` carries a `GraphClient`, a `PushMode`, an optional
`PushEndpoint`, a `shared_mailboxes: Vec<String>`, and
`public_folders: Option<PublicFolderScope>`.
`with_push_endpoint_client_state(url, secret)` is the ONLY webhook-mode
constructor: it selects `GraphSubscriptions` and sends the caller-owned
account-wide secret on every resource so the out-of-process receiver can
validate Graph notifications. `PushEndpoint::client_state` is a `String`, not
an `Option`. The removed `with_push_endpoint(url)` let `create_subscription`
mint a random per-resource value and drop it on the floor, producing
subscriptions no receiver could authenticate; nothing in the crate generates a
`clientState` any more. `with_ews_streaming()` selects `EwsStreaming`
and clears the endpoint; default webhook-mode without an endpoint makes
`push_subscribe` fail with `Unsupported(PushSubscribe)`.
`with_shared_mailbox(id)` registers a delegate/shared mailbox by its
`/users/{id}` routing key. `with_public_folders(scope)` opts in to
public-folder discovery (default off) and says which folders may SYNC:
`PublicFolderScope::hierarchy_only()` projects the hierarchy and syncs
nothing, `PublicFolderScope::pinned(ids)` syncs exactly those folders. The
argument is required, not defaulted - an org can carry thousands of public
folders holding millions of items, so "sync everything discovered" is a
defect, not a convenience.

`GraphClient` also derives an Autodiscover/EWS origin (`outlook_base`) from
its Graph api-base: a base on `graph.microsoft.com` keeps the production
`https://outlook.office365.com`, any other host (a harness mock, a sovereign
cloud) becomes the origin for `/autodiscover/autodiscover.{xml,svc}` and
`/EWS/Exchange.asmx` too. `with_outlook_base(base)` overrides it explicitly.
Without this, redirecting the Graph api-base left Autodiscover and EWS
pointed at the real service, so the public-folder and EWS-streaming legs
could not be exercised against a mock at all. An UNUSABLE api-base derives
an unusable Outlook origin rather than falling back to production, so a
misconfigured Graph base cannot leave Autodiscover and EWS sending the bearer
to the real `outlook.office365.com`; `with_outlook_base` can still name one.

### Which URLs may carry the bearer (`origin.rs`)

bifrost-net attaches the token source's bearer to the FIRST request of every
call, whatever its host; its trusted-host allowlist and `Authorization`
stripping apply only to redirect hops. So the origin decision is made here,
once. Each configured base (the api-base, the Outlook origin) is parsed into
a `Base` at construction: http or https, a host, and no userinfo, or it is
stored as unusable and every request on that surface fails closed before any
byte (`Request(Malformed)`, consumer configuration; the published
constructors stay infallible). A target is resolved against a base - a path
starting with `/` is concatenated (an RFC 3986 join would drop `/v1.0`), any
other text that parses as an absolute URL is taken as is, anything else is
concatenated with a slash - and the decision is made on the FINAL parsed URL:
exact origin equality with the base, no userinfo in the parsed fields or in
the authority as written. The result is an `AdmittedUrl`, sent as its own
serialization so the admitted URL is the sent one.

Every send path takes its transport from one accessor, `attached_net`, and an
EWS lane from `public_folder::ews_client`. A client with no attached
`AccountNet` is unreachable - the factory attaches before any `GraphAccount`
exists, `with_account_net` fills the slot at construction, derived clients
copy a filled slot, and nothing empties it - so an empty or poisoned slot
is `GraphError::Internal` /
`EwsError::Internal` -> `Internal(InvariantViolated)`. It used to be minted as
`Network(Unsent)`, which the engine retried forever although a retry of the
same request never attaches anything. The EWS streaming worker reports it as
`WatchEvent::Terminated` rather than exiting silently; best-effort
public-folder discovery reports it as an operator warning, like the rest of
that leg.

The bearer-carrying send paths accept nothing else: `execute_wire`,
`download_stream`, `EwsClient` (which holds its admitted endpoint or the
reason there is none), and `execute_aux`, whose `AuxTarget::Bearer` takes an
`AdmittedUrl` while `AuxTarget::Anonymous` (the pre-authenticated OneDrive
chunk PUTs and session cancel) takes an `UploadUrl` and never carries the
bearer. Only `Base::admit_upload` builds an `UploadUrl`, so no string reaches
the anonymous path unadmitted; its `Debug` names the origin and path only,
since the query holds the session's pre-authentication (see `cloud.rs` above
for the rule). Server links
deserialize as `ProviderLink`, which has no accessor yielding a requestable
string, so a link cannot be followed, minted into a caller cursor, or
persisted into a checkpoint without admission. Each provenance is admitted
where it is known, and classified there:

- A server `@odata.nextLink` / `@odata.deltaLink` is admitted at RECEIPT,
  before it is followed or minted; a refusal is `GraphError::ProviderLinkRefused`
  -> `Protocol(ContractViolation)` with `Acknowledged` evidence.
- A caller page cursor is admitted at decode (`paging::decode_paged_cursor`,
  `decode_link_cursor`, the search cursor's link against the owning mailbox's
  client); a refusal is `Request(Malformed)`. Bare link cursors must be
  absolute URLs; paged JSON cursors may also be a `/` path (older cursors
  named their first page that way). Invalid UTF-8 is malformed too, where two
  list surfaces used to answer `Unsupported`.
- A persisted delta link or page marker is re-admitted at resume
  (`cursor::admit_resume_url`); anything that is not an admitted absolute
  link is `CursorError::Decode` -> `SyncState(SchemaIncompatible)`, which
  reseeds the scope. Payload and marker constructors take an `AdmittedUrl`.

Graph documents its links as complete URLs on the service root that issued
them, and a national cloud is a separately configured base, so exact origin
equality breaks no documented flow. A reverse proxy that rewrites the base
but not the links Graph returns is the setup it does refuse.

Autodiscover `GetUserSettings` redirects are read from the protocol's own
`ErrorCode`, user level before response level (Exchange reports a per-user
redirect or refusal under a response-level `NoError`, and reading only the
first `ErrorCode` in the document once hid every per-user answer):
`RedirectAddress` names a mailbox, `RedirectUrl` an endpoint that must admit
onto the Outlook origin. A cross-origin `RedirectUrl` is
`Unsupported(Discover)`: the bearer was issued for the configured resource,
and a hybrid endpoint on another origin needs trust this client does not
establish. A self-contradicting answer (a code without a target, a target
without a redirect code, a URL named as a mailbox, an endpoint that is not an
http URL, or in POX an `Action` the protocol does not define) is the
provider's contract violation, `Protocol(ContractViolation)`: the document
parsed in full, so it is not a parse failure. A document that is malformed,
truncated or not an Autodiscover answer stays `Protocol(ParseFailed)`.

The POX (`autodiscover.xml`) delegate lookup follows the same rules as the SOAP
one. `<Action>redirectAddr</Action>` (a new mailbox against the same endpoint)
and `redirectUrl` (the same mailbox at an endpoint that must admit onto the
Outlook origin) are followed rather than read as "no delegates", which is what
a hybrid tenant needs to find its delegates at all. A `RedirectWalk` records
every (mailbox, endpoint) pair a lookup has asked, shared by the SOAP and POX
paths, so a redirect back to a pair already asked is caught before the repeat is
sent and is the provider's loop: `Protocol(ContractViolation)`. A chain of
DISTINCT hops longer than `MAX_REDIRECTS` is this crate's own safety cap,
`Internal(LimitExceeded)`, not the caller's input (it used to be
`Request(Malformed)`) and not proof of a provider fault, since a long chain may
be a legitimate deployment.

Failures Autodiscover reports inside an HTTP 200 are classified by what the code
says, never as the caller's input, since the lookup's only inputs are the
account's own address and the settings this crate names
(`graph_error::autodiscover_error_code_to_account_error`). `ServerBusy` is
`Server(RateLimited)` at account throttle scope; `InternalServerError` is
`Server(Unavailable)`; `InvalidUser` is `NotFound(Mailbox)`; `InvalidRequest` /
`InvalidSetting` are `Request(Malformed)`, the answer an HTTP 400 would get; and
`SettingIsNotAvailable`, `InvalidDomain`, `NotFederated` and any code this build
does not know are the provider declining to answer, `Server(Error)` with no
status, so `ProviderRefused`. All carry `Acknowledged` evidence and the verbatim
code as the native code. A setting that is simply absent with no per-setting
error to explain it is `Protocol(MissingField)`. POX numeric codes
(`autodiscover_pox_error_to_account_error`) map only the two whose meaning is
well established, `500` as `InvalidUser` and `600` as `InvalidRequest`; every
other code, and an `<Error>` with no code, is `ProviderRefused`, because
guessing a transient meaning for an unpinned numeric code would retry something
that may be permanent. A POX `<Error>` fails the delegate lookup with that
classification. Text runs that do not unescape are a malformed answer rather
than silently dropped, since dropping one would splice its neighbours into a
different address.

`AccountFactory::open(account_id)` attaches the `GraphClient` to `bifrost-net`
under the engine `AccountId`, validates the token with a `users/me` profile
fetch (`get_profile`, whose `mail`/`userPrincipalName` seeds the public-folder
Autodiscover lookups), constructs a `GraphAccount`, and runs
`list_mail_folders_recursive` to seed the `FolderTree`. That listing is then
handed forward in `open_folder_seed`, a one-shot slot the first
`discover_cursor_scopes` consumes instead of walking the hierarchy again: the
engine calls discovery moments after open, and the second walk answered
identically and re-seeded the same tree - one full recursive listing of pure
duplicate traffic per attach, unbounded in request count for a deep mailbox.
The slot is one-shot on purpose, so a discovery RE-RUN lists for real; noticing
folders created since open is exactly what a second discovery is for. Pinned by
`discovery_reuses_the_listing_open_seeded_and_lists_again_on_re_run`. Cursors
mint lazily from
`establish_initial_cursor` plus the first `inventory_stream` page. Returns
`OpenedAccount`; the only skip Graph records on its lane is a failed
opt-in delegate-Autodiscover pass (account-scoped, since discovery is
what failed and no narrower scope is knowable) - config-supplied shared
mailboxes always install.

`GraphAccount` owns:

- The shared primary `GraphClient` plus `shared_clients: Arc<HashMap<String,
  GraphClient>>`, one `for_shared_mailbox(id)` client per foreign mailbox.
  Every foreign operation, including initial inventory and delta-link resume,
  selects that owner client before it builds or follows a URL; continuations
  retain the selected client even though Graph's `nextLink` and `deltaLink`
  are absolute. Foreign folder and object ids are routed only through an owner
  present in that map, and an EMPTY routing key never enters it (`with_shared_mailbox("")`
  is constructible and used to install a client whose prefix was the
  malformed `/users/`, turning a local configuration error into an opaque
  remote 400; `merge_shared_mailboxes` already applied the same rule on the
  Autodiscover leg). A persisted id whose owner was removed from
  configuration is rejected locally rather than stripped and sent to `/me`,
  which would address a different mailbox namespace. Where that rejection
  lands depends on the surface's answer shape:
  - **Both cursor doors**, per scope: `initial_delta_url` refuses before the
    first delta request and `changes_stream` refuses before resuming a
    persisted `@odata.deltaLink`. The second is not redundant - the delta
    link Graph minted is namespace-correct, so that walk keeps SUCCEEDING
    for a mailbox this account no longer configures, leaving a live scope
    whose every object id fails hydration and mutation. Both classify
    `SyncState(ScopeRevoked)` carrying `ErrorScope::Cursor`, so the engine
    disables exactly that scope.
  - **Per-item batch surfaces** (`get_stream` hydration, `message_reactions`,
    the `bulk_*` mutation funnel), per id: the id is filed
    `Failed(Request(Malformed))` on its own lane through
    `batch_routing::partition_routable` and the rest of the chunk goes to
    the wire. A top-level `Err` there would discard valid siblings, and on
    `message_reactions` would additionally claim nothing was transmitted
    while earlier chunks had already completed.
  - **Per-request surfaces** (single-message `pim` writes and blobs), per
    call: the call fails, carrying the scope of the offending id.
  - **Push subscription**, per scope in BOTH push modes:
    `PushSubscription.outcomes` uses the shared three-lane batch contract, and
    its optional handle covers only the succeeded scopes. Every per-scope
    refusal routes through that lane - a poll-only public folder, a scope with
    no Graph subscription resource, a foreign-mailbox or non-`FolderType` EWS
    scope, and a refused or omitted id translation alike - so one bad scope
    never disables push for its valid siblings. `Err(_)` is reserved for
    whole-request faults: an empty scope list, a webhook mode with no endpoint,
    a request in which no scope was subscribable at all, and a transport
    failure during subscription creation (which rolls back what it created).
    The "no scope was subscribable at all" rule covers the scopes that die
    INSIDE an arm as well as the ones the pre-dispatch poll-only filter
    catches: an arm that resolved no resource (or whose every id was refused
    or omitted by `translateExchangeIds`) hands the dispatcher no handle, and
    the dispatcher turns that into `Err` rather than an `Ok(PushSubscription)`
    with no handle and an all-failed ledger - a shape a caller matching on
    success reads as partial coverage it cannot unsubscribe.
    `push/common.rs no_subscribable_push_scopes` builds that error by
    PROMOTING the first failed-lane entry verbatim and appending every other
    entry's causes as secondary chain evidence, so the per-scope diagnosis
    survives the ledger it is built from. The kind is derived rather than
    minted: jmap's `no_mappable_push_scopes` can name one kind
    (`Request(Malformed)`) because its whole population has one failure mode,
    while these arms mix `Unsupported(PushSubscribe)`, `Request(Malformed)`,
    `Protocol(ContractViolation)` and transport failures, and flattening a
    provider contract violation or a retryable 500 into `Request(Malformed)`
    would derive `ClientBug` for a fault the caller cannot fix.
- The built `AccountCapabilities`; the push mode plus optional endpoint; a
  `broadcast::Sender<WatchEvent>` for ordinary health edges and invalidations,
  plus a `watch::Sender<Option<AccountError>>` that latches terminal health for
  `push_stream` readers.
- An `Arc<RwLock<CursorIndex>>` (scope list) and `Arc<RwLock<FolderTree>>`
  (parent map).
- `HashMap<SubscriptionHandle, GraphSubscriptionGroup>` (webhook) +
  `HashMap<SubscriptionHandle, EwsSubscriptionState>` (EWS) + worker join slots
  and a `CancellationToken`.
- A bounded LRU `etag_index` of mail HTTP entity-tags (powering `If-Match`; mutation
  preflight reads refresh recency, `MESSAGE_SELECT` explicitly requests
  `changeKey`; the shared JSON projection prefers `@odata.etag` and otherwise
  wraps the raw change key as `W/"<changeKey>"`. Successful writes record a
  returned etag or evict when Graph returns none, 412 responses evict before
  retry, and moves, tombstones, 404 destroy skips, and confirmed destroys evict
  their old-id keys). It is
  a `HashMap<Arc<str>, _>` paired with a `BTreeMap<u64, Arc<str>>` of recency
  tickets, so insert / hit / evict are each one hash lookup plus a couple of
  tree nodes and never a scan: the write lock is taken once per hydrated page
  and once per mutation preflight, where a scanning policy would cost
  `messages * capacity` comparisons serialized behind it. Alongside it a
  per-mailbox `trash_folder_ids` cache resolves `deletedItems` once per
  opened account (primary key empty, shared key its routing key); a reopen
  creates a fresh cache. This avoids re-listing all well-known folders for
  every `delete_thread` while retaining owner-correct routing. ONLY a
  resolved folder id is cached, and a failed lookup propagates instead of
  degrading to the well-known name `deletedItems`: that literal is a valid
  move destination but not the id `container_is_trash` compares against, so
  answering with it turns "destroy a thread already in Trash" into a
  no-op move that reports success - and caching it made one transient
  failure do that for the rest of the account's life. Alongside it
  is a
  `routing_map: Arc<RwLock<HashMap<FolderId,
  PublicFolderRouting>>>` from public-folder discovery, plus
  `public_folders_enabled` and the discovered `user_email`.
- `set_priority` / `set_bandwidth_cap` delegate to `AccountNet`.
  Open attaches before account construction; same-id reattachment preserves
  the bandwidth budget, while cross-id retagging selects the destination budget.

### Per-batch byte accounting

`GraphClient` carries an optional `ByteTally`, and `GraphAccount::metered()`
hands back an account view whose primary client AND every shared-mailbox client
report into one fresh accumulator - a foreign-mailbox request belongs to the
batch that made it. The record sits at both wire funnels (`execute_wire` and
`execute_aux`), so the pre-authenticated chunk PUTs, the session cancel and the
Autodiscover POST are counted too. Each engine stream takes one accumulator and each emitted batch
`take`s it.

`metered()` is a `self.clone()` with two client fields swapped, so it produces
a SECOND `GraphAccount` value over the same account. Every field that must be
one thing per account has to be `Arc`-shared for that to hold - which is why
the maps, the worker slots, and the `push_disconnected` health latch all are.
A `watch::Sender` is itself a clone-shared handle, so the terminal push latch
also stays one value across the metered view.
A plain (non-`Arc`) field carrying account state silently FORKS across the
metered view: the metered half would mutate its own copy, and the two halves
would then disagree about, say, whether push is currently down - a divergence
nothing in the type system flags. The byte tally itself is the one field that
is deliberately per-view.

Both funnels record from the request-local `RequestByteCounter` they hand to
`RequestBuilder::count_bytes_into`, not from the `Response`, so a FAILED request
contributes its drained bytes as well. That is load-bearing rather than tidy:
the hydration and mutation lanes convert a request error into per-item failures
and still emit a batch, so a success-path-only record would report zero for
exactly those batches. `RestResponse` consequently carries no byte count of its
own.

One batch is many requests here: a mutation chunk is the `$batch` submission
plus one etag GET per id the preflight had to re-read, and a delta walk pages.
The declaration also carries the engine `AccountId` as its `quota_scope`, so the
Graph per-tenant bucket is per account rather than pooled process-wide.

`EwsClient` composes `AccountNet` directly rather than routing through a Graph
REST funnel. Its buffered `execute` funnel therefore accepts the metered
account view's existing `ByteTally` and records each request's drained bytes
there, on the success and error paths alike. Public-folder inventory, changes,
and hydration consequently share the same batch accumulator as any REST half of
the operation. The long-lived
`GetStreamingEvents` funnel deliberately does not feed a batch tally: its
streaming counter is complete only when fully drained, and push traffic is not
an engine batch.

Reopen is engine-delegated: on drop or after `close()`, the engine calls
`GraphAccountFactory::open` again for a fresh `GraphAccount` (empty caches, a
fresh push terminal latch, and a fresh shutdown token); the factory holds the
client, so the new account reads the token source's current value.
`push_stream` wraps the broadcast receiver and terminal watch receiver in a
`stream::unfold` selecting against the same token. Terminal health is stored
before its broadcast send, so a subscriber arriving later reads the latched
error, and a lagged subscriber reads it instead of losing it to ring overflow.

`close()` retires SERVER-side state before it stops any worker, because neither
kind of subscription dies because this process did. In order:
`retire_all_graph_subscriptions` cancels the shutdown token (its first step,
before it looks at any group, and `close()`'s only cancel), then walks
`graph_subscriptions` and runs the teardown per handle (best-effort - a DELETE
failure is logged, never returned, since `close()` must still complete and the
engine has no recovery for "the server kept a subscription"); `close()` then
JOINS the EWS worker under `CLOSE_WORKER_JOIN_TIMEOUT` rather than aborting it,
so the worker's own `Shutdown` arm gets to send its EWS `Unsubscribe` (aborting
preempted exactly that, and Exchange caps streaming subscriptions per mailbox,
so every reopen burned one until it timed out); and finally JOINS the renewal
worker under the same budget, aborting only on timeout (see "Cancellation
safety of the renewal recreate"). Without the walk, each reopen stranded one
live webhook subscription per resource still POSTing to the consumer's
receiver.

`run_worker`'s `Shutdown` and `Terminated` exits both call
`release_subscription`, like the `Resubscribe` / `Disconnected` exits.
Leaking on a terminal exit is worse than leaking on a reconnect: nothing in
that worker's remaining lifetime comes back for it.

A broadcast `Lagged` on that receiver yields a synthesized
`Invalidated { source: Coalesced, payload: Unknown }`, not a `continue`. The
dropped events are the only record that those scopes changed and are never
replayed, so swallowing the overflow converts a recoverable burst into
indefinite staleness for the affected folders; a coalesced whole-account
invalidation is exactly what `PushSource::Coalesced` exists for.

## Capabilities

`build_capabilities(push_mode)` in `capabilities.rs`:

- `cursor_freshness: ServerIssued`. Graph mints `@odata.deltaLink`; the engine
  persists and resumes it.
- `blob_range: Conditional` (per-handle: fileAttachments support `Range` against
  `/$value`, item/referenceAttachments do not, carried by
  `BlobHandle::capabilities::supports_range`); `blob_digest_pre_download: false`.
- `push` depends on `PushMode`: `GraphSubscriptions` -> `WebhookOrEwsStream`
  (out-of-process: subscription CRUD on the Account, HTTPS receiver wired by the
  consumer into the engine `InvalidationSink`; `push_in_process()` false);
  `EwsStreaming` -> `InProcess` (the EWS worker forwards `Invalidated` on
  `push_stream`; `push_in_process()` true).
- `mutation.concurrency: ExpectedVersion { contact_update: true, event_update: true }`.
  This caller-version guarantee is limited to these update doors. Bulk mail
  retains provider-observed guards: every non-`Destroy` mutation sends
  `If-Match: <entity-tag>`; the cached etag comes from inventory/changes/get,
  refreshed from `messages/{id}?$select=id` on a cold cache.
- `mutation.replay_safety: None`. No client-mintable replay token; the read-back
  guard is the lost-update net.
- `batching_policy: { max_items: 20, max_wait: 100ms, flush_on_input_close: true }`
  (the 20 ceiling matches Graph's `/$batch` limit).
- `rate_limit_class: Tiered` (per-mailbox concurrency + per-app throttle budget);
  `quota_signal: RetryAfter` (`Retry-After` becomes `Retry`'s `not_before`).
- `requires_uidvalidity_recheck: false`; `historyid_expires_after: None`;
  `delta_token_expires_after: None` (expiry is reactive: 410 Gone / 400
  InvalidDeltaToken).
- `pim_methods`: true for the container/category/extended-property/
  importance/is-read writes, send/draft lifecycle, `scheduled_send`,
  search, mail folder CRUD, `identities_list`, vacation, typed hydration,
  contact/calendar primitives, `directory_search` (org directory via
  `/users`), and `host_attachment`. False for
  `remove_from_container`, `set_keyword`, `set_label_membership`,
  standalone `attachment_upload`, `identity_update`, `quota_get`.
  `send_as` is the one flag derived per account rather than fixed: true
  only while `shared_clients` is non-empty, refreshed once if delegate
  Autodiscover widens that map during `open`.
- `filter_rule_shape: Rules`; all 5 filter flags true (Graph Inbox
  `messageRules` CRUD + local `filter_validate`).
- `conveniences`: `starred = Category` (reserved `$flagged` ->
  `flag.flagStatus`); replied/forwarded dispatch to `set_extended_property`
  with `PidTagLastVerbExecuted` 102/104; `mdn_sent_via_keyword = false`
  (Graph `isReadReceiptRequested` read-only -> `Unsupported(MarkMdnSent)`).

## Cursor envelope

`OpaqueChangeState` for Graph is tagged `ProtocolKind::Graph` with
`envelope_version = GRAPH_CURSOR_ENVELOPE_VERSION` (currently `4`);
`CHANGE_CURSOR_ENVELOPE_VERSION` is the matching
`ChangeCursor.envelope_version`. v2 is an OBJECT-ID encoding change, not a
payload-shape change: v1 minted bare `ThreadId`s for shared mailboxes, and
those still parse - as PRIMARY - so no additive `serde(default)` field can
detect them. A v1 account would resume its delta link, never re-run
inventory, and keep routing thread hydration and thread-targeted writes at
`/me`. `decode_cursor` therefore refuses a v1 cursor as
`SchemaIncompatible`, which derives `Engine(SchemaIncompatible)`: the engine
drops every durable cursor and re-establishes each scope through a full
`inventory_stream` pass, which re-mints the ids. The ids are server-issued
and not reconstructable from the stored bytes, so reseeding is the migration.
v3 added the calendarView window end to the payload. Graph retains that window
inside every delta link it mints, so a cursor reseeds ninety days before its
one-year end horizon rather than silently losing future events. The bound
cannot be reconstructed from a v2 link safely, so v3 is also a reseed boundary.
v4 adds the `inventory_in_progress` payload state: a page checkpoint has no
delta link yet, so bifrost-sync recognizes it at attach and resumes
`inventory_stream` at its saved next link rather than starting `changes_stream`.
`GraphCursorPayload` carries a kind, final `@odata.deltaLink`, optional
calendar window end, and optional mid-walk page marker
(landing in `OpaqueChangeState::bytes`; page markers also in
`ChangeCursor::advanced_through`).

`decode_cursor` rejects wrong protocol, incompatible envelopes, and malformed
JSON. The OUTER `ChangeCursor::advanced_through` is the single source of truth
for page progress in both directions: it overrides the payload's copy when
present (the engine persists it separately and may have acked a later page than
the bytes recorded) and CLEARS it when absent (it may equally have acked no page
at all, and a marker the engine never acknowledged must not pick the resume
URL). Overriding on presence but deferring on absence gave one field two
sources with a silent precedence; `encode_cursor` writes both from the same
value, so honoring the outer unconditionally loses nothing.
When an in-progress inventory payload has no outer marker, the inventory walk
returns `CursorError::InventoryInProgress`, which derives a restart of that
scope. The engine acknowledged no page position, so this does not clear every
scope as a schema reset would.
`changes_stream` cross-checks that payload kind projects back to
`cursor.scope`; mismatches terminate with `SyncState(SchemaIncompatible)`.
`establish_initial_cursor` accepts delta-eligible `FolderType` scopes (email,
event/calendar event, contact) plus any `CursorScope::Folder` in the
public-folder routing map; both mint the first cursor through inventory. A delta
`describe_cursor` is cheap/`ServerCursor`/fresh; a public-folder cursor is
cheap/`Poll`/fresh; invalid cursors reseed through inventory.

### Public-folder cursor (no delta token)

Graph cursor envelope v5 invalidates the former public-folder baselines,
which included non-mail items. The decoder maps payload decode failures to
account-wide `SchemaIncompatible` recovery, so a payload-specific version
would not isolate recovery. The shared version deliberately reseeds all
Graph scopes. The payload IS the sync state:
`folder_id`, `PublicFolderRouting` (the cold-resumable `X-AnchorMailbox` /
`X-PublicFolderMailbox` pair), a `DateTimeReceived` `watermark`, the
`last_full_scan_at` throttle clock, a `live_ids` deletion baseline, and a
`live_versions` change-key baseline.
`public_folder_changes_stream`: incremental `find_items` since the
watermark (emit `Updated` + `Folder` and `Mailbox(content)` `Added` scope
changes, matching the inventory pass's dual membership), then a deletion
reconcile throttled to `FULL_SCAN_INTERVAL_SECS` (3600). Every complete full scan reconciles the admitted mail projection: a retained
id with a changed key emits `Updated`; a previously admitted item now absent
or non-mail emits `Destroyed`; a newly admitted mail item emits the object
and both memberships even below the watermark. An excluded item disappearing
emits nothing. Incomplete walks cannot advance or reconcile this baseline. The EWS item shape carries read state, flag status,
and categories through the same canonical token hash used by REST inventory.
Incremental admissions, timestamped or not, join the capped id and version
baseline before checkpointing, so a later scan does not re-emit them.
`live_ids` is hard-capped at `PUBLIC_FOLDER_LIVE_IDS_CAP` (10_000); above
it the snapshot empties and the folder degrades to additions-only with one
scoped `Warning`. `DateTimeReceived` is not a modification signal, so an
in-place edit that does not move an item's received time is invisible to the
incremental poll: it surfaces only on the next complete scan, up to
`FULL_SCAN_INTERVAL_SECS` late, and not at all while the folder is in the
over-cap degraded mode (which empties `live_versions` along with `live_ids`).
That is a limit of the poll model - closing it needs a different watermark
field (`LastModifiedTime`) or a different model - not a regression. Dispatch: routing-map membership for `Folder` scopes
(establish/inventory), cursor kind for changes; a bare non-public `Folder`
keeps its reject-on-delta behavior. A `Folder` scope is refused
per scope as `Unsupported(PushSubscribe)` in `push_subscribe`'s failed lane
(poll-only v1); its subscribable siblings still get a handle. Lost rights
surface as EWS `ErrorAccessDenied` and quarantine just that scope via
`ews_shared_scope_error` -> `ScopeRevoked` -> `DisableScope`.

## Per-scope inventory, changes, hydration

Supported scopes:

- `FolderType { folder, Email }` -> initial URL
  `/{prefix}/mailFolders/{folder}/messages/delta?$select=...&$top=50`
  (`MESSAGE_SYNC_SELECT` for a fresh inventory). It selects only id,
  parentFolderId, read/flag state, categories, changeKey, conversationId,
  internetMessageHeaders and internetMessageId. The broader `MESSAGE_SELECT`
  remains for typed message hydration. Microsoft documents `$select` for
  message delta and says initial query options are encoded in returned links,
  so an already persisted delta link keeps the projection it was minted with
  until that cursor is reseeded ([message delta documentation](https://learn.microsoft.com/en-us/graph/delta-query-messages)).
  `inventory_entry_from_value`
  pulls id, conversationId
  (thread), change-key (etag), Message-ID / References / In-Reply-To from
  `internetMessageHeaders`, and a flags hash from `isRead` / `flag.flagStatus`
  / `categories`. `size` is `None` on this projection.
- `FolderType { folder, Event | CalendarEvent }` -> calendarView delta over
  a [-90d, +365d] window; its cursor records the end and is reseeded with a
  new sliding window ninety days before that end; thread/message-id fields
  empty. The initial URL has no `$select`, because Microsoft's
  [calendar event delta documentation](https://learn.microsoft.com/en-us/graph/delta-query-events)
  says that parameter is unsupported and documents the `odata.maxpagesize`
  preference. Inventory and changes requests send
  `Prefer: odata.maxpagesize=50, outlook.timezone="UTC"` on every page; the
  [event GET documentation](https://learn.microsoft.com/en-us/graph/api/event-get?view=graph-rest-1.0)
  defines the UTC timezone preference.
- `FolderType { folder, Contact }` -> initial URL
  `/{prefix}/contactFolders/{folder}/contacts/delta?$select=...&$top=250`
  (`CONTACT_SELECT`).
- `Folder(folder)` in the public-folder routing map -> the
  `public_folder.rs` poll strategy (see "Public-folder cursor"); a
  `Folder` absent from the map stays `Unsupported`.
- Any other scope / type makes `initial_delta_url` return an error that
  `inventory_stream` projects into `Terminated(Unsupported(_))`;
  `establish_initial_cursor` gates scopes before that point.

The inventory walk reads pages via `get_json` / `get_absolute` (the
`@odata.nextLink` chain), yielding `PageBoundary::Page` Batches with a
consumer-acknowledged `inventory_in_progress` checkpoint until a `delta_link`,
then a `Final` Batch + ordinary `Checkpoint::Change`. The payload stores the
next link without inventing a delta link, and `changes_stream` rejects that
state defensively. A page with neither link is a contract violation, never a
successful cursor-less completion.
`@removed` entries populate `InventoryBatch::removed` using the same scoped
object ids as live entries, and mail removals evict their cached change key.
Harvested etags fold into the bounded LRU `account.etag_index` for the next
`If-Match`. Both engine inventory paths emit these tombstones as
`ScopeChange::Removed` in the same batch as the page's accepted checkpoint.
The consumer persists the removals and checkpoint atomically before acking,
exactly as for a removal on the live change stream.

`changes_stream(cursor)` decodes the payload, asserts `scope_matches_payload`,
and walks `delta_link` (or `advanced_through.next_link` on resume). Each
non-removed entry emits `ObjectChange { Updated }` + `ScopeChange { Added }`
(Graph delta has no created/updated split, and the account keeps no complete
per-scope id set to diff against); `@removed` emits one `ScopeChange
{ Removed }`. The live engine driver forwards object kinds unchanged. Under the shared
sync contract, `Updated` is an upsert: an unknown id is a create and
`Created` is only an optional precision hint. Its page marker's `last_seen_id` uses the same
projected id form as inventory markers, including calendar composites and
foreign mailbox tags; the id is diagnostic, and resume follows only
`next_link`. Page batches
checkpoint `advanced_through` at the next link; the final page checkpoints the
fresh `delta_link`, `advanced_through` cleared.

`get_stream` chunks ids into `batching_policy.max_items` blocks, fires one
`/$batch` per chunk, and returns exactly one per-id `ItemOutcome<HydratedObject>`:
2xx ->
`Succeeded`, 4xx/5xx -> `Failed` with a structured `AccountError` (via
`response_to_account_error_pub`), 2xx-no-body -> `Failed(Protocol(MissingField))`.
Locally-invalid items `Failed` without poisoning the batch; a whole-request
transport drop terminates. "Locally-invalid" includes an id that cannot be
ROUTED (`batch_routing::partition_routable` over `hydrate_url_for_id`): the
split runs before the POST, so a stale shared-mailbox id costs itself an
outcome and neither its valid REST siblings nor the EWS outcomes already
fetched for the same chunk. The subrequest index is assigned AFTER that
split, so it always indexes the ids actually sent.

`reconcile_hydration_responses` is the pure projector that enforces the
one-outcome-per-id contract, and `reconcile_mutation_responses` is its
mutation twin. Both discard a response id that does not parse as a submitted
index, parses out of range, or repeats an already-answered one - minting an
`ObjectId` from the response id would put an id the caller never submitted on
a lane while the real one stays unanswered. Both then sweep the ids Graph
never answered.

An unanswered subrequest classifies `Protocol(PartialResponse)` with
`TransmissionState::Acknowledged` (`graph_error::batch_response_missing`), on
all four `$batch` paths - hydration, the bulk mutation funnel,
`pim::messages::submit_write_batch_with_targets`, and the reaction read's
`classify_chunk` (whose unanswered ids ride the `BatchOutcome` uncertain
lane). The outer envelope decoding is
evidence about the envelope only: a `move` or `permanentDelete` that committed
and lost its subresponse is indistinguishable from one that never ran, so the
error must not assert the item was left alone. `ContractViolation` would
derive `ProviderContractViolation`, which is terminal, and the engine would
file the mutation `failed_terminal` with no read-back. `PartialResponse`
instead retries the idempotent operations (hydration, `UpdateFlags`) and
routes the non-idempotent ones (`BulkMove`, `BulkDestroy`) to
`Reconcile(PartialCompletionSignal)`. In the bulk funnel the ambiguous id
also rides the **uncertain** lane rather than `failed`, so the engine's
read-back guard resolves it.

`pim::messages::submit_write_batch_with_targets` is the one `$batch` path
that answers per REQUEST - its callers are single-`Result` trait methods -
but it still
DRAINS every subresponse before returning. `$batch` response order is
Graph's, not the caller's, so returning on the first bad item left the etag
of a message that demonstrably was destroyed in the LRU whenever its
subresponse sorted after a failing sibling's, and a later conditioned write
on that id then sent an `If-Match` for a message that no longer exists. The
error surfaced is unchanged: the first failure in response order.

`message_reactions` (`reactions.rs`) reads the two Outlook
`singleValueExtendedProperties` (`OwnerReactionType`, `ReactionsCount`) over
the same chunked `$batch`, dedupes its ids, and answers every submitted id in
exactly one lane. Public-folder ids name EWS items this Graph-only
extended-property surface cannot address, so `partition_supported_ids` splits
them off and files each one `Failed(Unsupported(MessageReactionsRead))`
locally while the Graph ids of the same batch still go to `$batch` - a
top-level rejection would be a per-request answer on a per-item surface and
would discard the outcomes of every ordinary message beside the public id.
An unroutable id (stale shared mailbox) files the same way, through
`batch_routing::partition_routable` per chunk. Here the boundary contract
adds a second reason: past the 20-id chunk limit earlier chunks have already
been transmitted, so a top-level `Err` - which means "nothing was
transmitted" - would be false as well as lossy.

Nothing Graph sends can make the ledger's `finalize` fail here (every lane id
is a deduplicated caller id, and `classify_chunk` bounds-checks and
deduplicates response positions before pushing), so a `finalize` failure is
this module's bug: `GraphError::Internal` -> `Internal(InvariantViolated)`,
not a provider contract violation. The push door's ledger
(`push/dispatch.rs finalize_push_outcomes`) classifies the same way for the
same reason: every arm files each positional id exactly once from its own
scope list, and server answers are looked up by key, never iterated into the
ledger. Each push arm calls `finalize_push_outcomes` itself, before it inserts a
handle or starts a worker (see the push section), so a `finalize` failure is
raised while nothing is live.

The `hydrated_from_value` projector maps `FlagsOnly`
-> canonical flag `HashSet`; `Metadata`/body-bearing -> `metadata_or_flags`.
Graph JSON is not assembled RFC822, so body-bearing projections degrade to
`Metadata` (assembled bytes come from `open_raw_rfc822`).

### `get_stream` is the mail hydration lane

`hydrate_url_for_id` builds `{prefix}/messages/{id}` for every id, and
`select_for_projection` returns message-shaped `$select` lists for every
projection - deliberately, even though this account also establishes
`ObjectType::Event` and `ObjectType::Contact` cursor scopes and mints their
inventory entries and change ids through the same generic pipeline. Event
inventory entries and changes (including removals) use `{calendar}::{event}`,
the same identity accepted by the typed calendar doors. Contact ids remain
native Graph contact ids. Only email scopes populate or evict the mail etag
cache. Foreign event/contact scopes are refused before fetching: the typed
non-mail doors currently read the primary mailbox and cannot consume a shared
mailbox identity safely.

The determination, from the engine: `bifrost-sync` never routes event or
contact scope ids into `get_stream`. Its only internal callers are the
mutation read-back paths (`mutation/readback.rs`), which hydrate at
`FlagsOnly` / `Metadata` the ids a caller just put through the mail mutation
API. `SyncEngine::get_stream` is otherwise a consumer-facing passthrough.
The workspace-wide contract agrees: the standalone calendar and contact
account crates (`bifrost-caldav`, `bifrost-carddav`), which produce event and
contact ids and nothing else, answer `get_stream` with
`unsupported_stream(AccountOperation::Hydrate)`. Calendar events and contacts
are read through their own typed surfaces instead - `calendar::get` by
`EventId`, the contacts API by contact id - which address `/events/{id}` and
`/contacts/{id}` themselves.

Note also that an `ObjectId` carries no object-type marker, so this lane
could not route by scope kind even if it wanted to; "non-mail ids do not
arrive here" is the contract, not an inference. Pinned by
`every_hydration_projection_selects_message_fields_on_a_messages_url`, which
fails if a non-mail `$select` field or a non-`/messages/` URL appears.

`scope_lifecycle_stream` is empty: Graph exposes no folder-lifecycle
notification surface; discovery re-runs on account reopen.

## Foreign (shared/delegate) mailboxes

A configured shared mailbox surfaces as ordinary
`CursorScope::FolderType { folder, Email }` scopes; the owning mailbox
identity rides inside the `FolderId` string via the `account/foreign.rs`
codec (`encode_foreign(mailbox, folder)` joins on `\u{1f}`,
`parse_folder` splits it back into `ParsedFolder::{Primary, Foreign}`).
No new `CursorScope` variant. `client_for_scope(scope)` selects the
`shared_clients` entry when the folder parses foreign, else the primary
client; `owner_of_scope(scope)` returns the `MailboxId` owner for a foreign
scope, `None` for primary. `discover_cursor_scopes_inner` lists the primary
then each shared mailbox's folders, emitting foreign-namespaced `FolderType`
scopes; a per-mailbox permission denial skips with a `Warning`.
`discover_memberships_inner` emits the foreign `MembershipScope::Mailbox(owner)`
tag with the folder membership, and `inventory_stream` stamps it onto every
foreign-scope item (the engine covering rule cannot form it).
`containers_list` resolves each shared mailbox's well-known folder roles on
that mailbox's own client (`well_known_folder_roles`, six `$select=id` GETs per
shared mailbox at listing time, not per operation), so a shared Inbox / Sent /
Drafts / Trash / Spam / Archive carries its `FolderRole` whatever its display
name. Before nc-4 (ruled 2026-09-07) the shared leg passed an empty role map
and fell back to matching the well-known NAME against the folder id, which
never fires for a real id, so shared mailboxes carried no roles and the
consumer's destructive-action routing (which folder is Trash, where the sent
copy files) had nothing to go on. Pinned by
`shared_mailbox_roles_are_resolved_on_the_shared_client`.
`initial_delta_url` reads the prefix from `client_for_scope` and the native id
from `parse_folder`, so the mailbox rides in `/users/{id}`, the id in
`/mailFolders/{id}`.

Per-message routing rides the same codec applied to the *message* id.
`foreign.rs::encode_message_id(scope, native)` foreign-encodes the message id
(`{mailbox}\u{1f}{native}`) when the scope parses foreign, and leaves a primary
message bare; `parse_message_id` -> `ParsedMessageId::{Primary, Foreign}` with
`.native_id()` / `.owner()`. The id is minted encoded at every projection site
(`inventory_entry_from_value`, and the `changes` Added/Updated/Removed ids) so
one logical message carries identical bytes everywhere - the consumer uses it
as a stable primary key, and a shared-mailbox message never also surfaces bare
via `/me`. Every per-message request decodes it and routes via
`client_for_owner(parsed.owner())` using `parsed.native_id()`: hydration
(`get.rs hydrate_url_for_id`), blob + raw (`blob.rs`), mutations (`mutate.rs`
flag/move/destroy and the `pim/messages.rs` writes), and the typed `message_hydrate`. A
foreign id builds `/users/{owner}/messages/{native}`, a primary id
`/me/messages/{id}`.

The THREAD-keyed doors carry the owner the same way. A `ThreadId` is
`encode_thread_id(scope, conversationId)` - `{mailbox}\u{1f}{conversationId}`
for a foreign scope, bare for the primary - minted at the same projection
site as the message id, and `parse_thread_id` -> `ParsedThreadId::{Primary,
Foreign}` decodes it. `message_values_for_thread` selects
`client_for_owner(parsed.owner())` and filters on `parsed.native_id()`, so
`thread_hydrate` and every `MutationTarget::Thread` fan-out
(`resolve_target_ids` / `resolve_target_values`) query
`/users/{owner}/messages?$filter=conversationId eq ...`, and the member ids
they hand back are re-qualified with that owner so the following `$batch`
subrequest stays in the same mailbox. `delete_thread` scopes the WHOLE
operation, not just the lookup: Trash is resolved and cached through the
owner's own `deletedItems` endpoint (a shared mailbox's id is not the
primary's) and returned owner-qualified, and the already-in-Trash
short-circuit compares in that namespace - a bare `deletedItems` is the
PRIMARY mailbox's Trash and must not send a shared thread down the destroy
branch. Conversation ids are unique per mailbox only, and Graph answers a
filter for an absent conversation with an empty 200, so the untagged form
was silent: hydration returned an empty thread and a thread-targeted write
reported success having touched nothing.

The owner also rides back out of the typed projection. `message_from_value`
takes the owner decoded from the requested id and qualifies all three ids it
mints - `Message.id`, `Message.thread_id`, and `Message.containers` (Graph
returns `parentFolderId` bare even from `/users/{owner}`) - through the
single `foreign::qualify_with_owner`. Without the container leg a shared
hydrated message could not join the `Shared`-namespace containers
`containers_list` emits (that path uses `encode_foreign`) and its parent id
was indistinguishable from a primary folder's.

`bulk_move` also decodes the destination
`FolderId` (the `destinationId` body must be the native id) and rejects a
cross-mailbox move - destination owner != source owner - as `Request(Malformed)`,
since one endpoint can't express it. The PIM `move_messages` path (reached from
`add_to_container` and `delete_thread`) refuses the same condition the same way,
decided from the ids alone and before its etag GET. The etag cache stays keyed by the encoded
id; only the URL uses the native id. `push/webhook.rs resource_for_scope` routes a
foreign subscription via `client_for_scope` + the native folder id, never
percent-encoding a raw `\u{1f}` id into the URL.

Revocation isolation: `graph_shared_scope_error(error, scope, owner, ctx)`
quarantines just the foreign scope when the failure is
`Authorization(PermissionDenied)` and `owner.is_some()` ->
`graph_scope_revoked` -> `ScopeRevoked` -> `Engine(DisableScope(scope))`. A
primary scope (`owner == None`) stays terminal `NoPermission`. Wired at the
`inventory_stream`/`changes_stream` per-scope fetch-failure boundary.

Foreign-mailbox enumeration is config-supplied (`with_shared_mailbox`) or,
opt-in, Autodiscover-enumerated: Graph REST has no "list my delegated
mailboxes" call, so `open` falls back to the EWS-era Autodiscover
`alternativeMailboxes` response when `with_delegate_discovery()` is set
(default off). `discover_shared_mailboxes` (`autodiscover.rs`) queries it for
the primary user's SMTP; `merge_shared_mailboxes` combines the result with
any `with_shared_mailbox` entries additively, empty-dropping and
exact-string-deduping across the whole merged set (config first, discovered
appended). An HTTP 200 whose body is malformed or truncated XML, or not an
`Autodiscover` document at all, is the provider's parse failure
(`GraphError::Json` -> `Protocol(ParseFailed)`), never an empty or partial
mailbox list; both Autodiscover parsers (`parse_alternative_mailboxes`,
`parse_user_settings_response`) require one complete document with the
expected root. The same holds for an in-body POX `<Error>`: it fails
`discover_shared_mailboxes`, classified by its code, instead of reading as a
user with no delegates, and POX `redirectAddr` / `redirectUrl` answers are
followed under the same loop detection and hop cap as the SOAP lookups (see the
Autodiscover paragraphs under "Which URLs may carry the bearer"). Discovery at
`open` stays best-effort and non-fatal: that failure, or a request failure,
degrades to the config-supplied mailboxes
rather than failing `open`, and the skipped pass is recorded on
`OpenedAccount::skipped_scopes` with its classified error so the
degradation is reportable, not just logged. The EWS twin `ews_shared_scope_error` applies the same
`ScopeRevoked` -> `DisableScope` isolation to public-folder scopes.

## Public-folder discovery (Autodiscover)

Opt-in via `with_public_folders(scope)`. After the primary/shared mailboxes,
`discover_cursor_scopes_inner` calls
`public_folder::discover_public_folder_scopes`: resolve hierarchy routing via
`GetUserSettings`, browse recursively from `find_folder("publicfoldersroot")`
with hierarchy headers (`child_folder_count > 0` re-enters the worklist; a
`visited` dedup set plus a browse-step cap bound the walk), read-gate via
`effective_rights.read`, and per folder resolve the content mailbox
(`get_folder` PR_REPLICA_LIST GUID -> `construct_replica_smtp` ->
`discover_content_mailbox`).

Neither Autodiscover leg may drop a folder. Both degrade instead, because
both depend on optional Exchange machinery that a tenant (or a harness) may
simply not serve, and dropping meant an EMPTY `routing_map`: zero public
containers, zero pinned scopes, and nothing but a support-only warning:

- No `PublicFolderInformation` (the setting is absent, or the lookup failed,
  a malformed answer included): `hierarchy_routing_fallback` anchors the
  browse on the account's own mailbox (a real identity - the anchor becomes
  the owner `MailboxId` on every emitted item, so it must never be empty).
- No content-mailbox routing for a folder: `content_routing_or_hierarchy`
  seeds it with the hierarchy routing the browse already succeeded with.

Both helpers are pure and unit-pinned. A `find_folder` failure at the root is
still fatal to the leg (there is no hierarchy at all); a sub-folder browse
failure still skips that subtree.

`seed_and_scope` then splits discovery from sync. It ALWAYS seeds two maps -
`routing_map` (the content-mailbox routing the cursor payload carries) and
`public_folder_meta` (display name, `FolderClass`, parent, effective rights,
for the `containers_list` projection) - and emits a `CursorScope::Folder`
only when the configured `PublicFolderScope` names a mail folder
(`IPF.Note` or a dot-separated subclass). Non-mail folders remain in
hierarchy metadata with their `ContainerContentClass`.
`HierarchyOnly` emits none. Both maps are filled synchronously inside
`SyncEngine::attach`'s discovery call, which is what makes them populated by
the time `containers_list` runs; neither seeding may be deferred to a lazier
point. Rights stay advisory at discovery (the authoritative gate is the live
`ErrorAccessDenied`), but they are now also PROJECTED onto
`Container::rights` so a read-only public folder is distinguishable
downstream.

## Public-folder hydration (EWS `GetItem` / `GetAttachment`)

A public-folder item is a raw EWS `ItemId`; Graph REST has no route that can
address it. `public_folder.rs` therefore mints folder-qualified object ids
(`foreign::encode_public_item_id`, an RS-separated `"<folderId>\u{1e}<itemId>"`
distinct from the US-separated shared-mailbox form) in both the inventory
projection and the poll's `Added`/`Destroyed` emissions.

`get_stream` splits each chunk with `partition_ews_ids`: an id whose folder is
present in `routing_map` goes to `fetch_ews_outcomes` (one `GetItem` per item,
because per-folder routing headers differ), everything else to the Graph
`/$batch`. Both arms land in one `Batch`, so the consumer still sees exactly
one outcome per pulled id. `hydrated_from_ews_item` projects into the SAME
`HydratedObject` shape the REST arm returns: `Metadata` (the identical
inventory-entry projection) for the body-bearing projections, `FlagsOnly` for
`FlagsOnly`, plus one blob handle per attachment descriptor. Body-bearing
projections degrade to `Metadata` for the same reason the REST arm does -
`RawMime` means assembled RFC822, and `GetItem` returns a parsed HTML body.

Attachment bytes route through `open_blob`: the blob locator carries the
folder-qualified message id, so `GraphBlobKind::Ews` fetches via SOAP
`GetAttachment` (base64-inline, hence no range support) and
`GraphBlobKind::EwsItem` surfaces the `BlobNotByteStream` warning.

## Push: webhooks plus EWS streaming fallback

Push has two modes, selected on the factory and reflected in the
capability surface.

### Webhook mode (`PushMode::GraphSubscriptions`)

`push_subscribe(scopes)` rejects an EMPTY scope list as `Request(Malformed)`
before mode dispatch, so in both modes (a subscription covering nothing
registered a group teardown could never retire and started a renewal worker
nothing could stop; the engine already skips empty lists at its own reattach
boundary). It then groups by Graph
subscription resource, files any scope with no subscribable resource into the
failed lane, creates one server
subscription per resource, and best-effort deletes any already-created
subscriptions if a later create fails or the outcome ledger cannot be closed
(`roll_back_created`). Closing the ledger (`finalize_push_outcomes`) is the
webhook arm's own step, taken after the last create and BEFORE the group is
registered or the renewal worker started. It used to run in the dispatcher
after the arm returned, so a `finalize` failure returned `Err` - no handle -
over subscriptions the worker kept renewing and no caller could name to
unsubscribe. Now that failure path has registered nothing and only rolls back
the server-side creates; a DELETE that fails there is logged and left to
Graph's own expiry, since no group holds the row. The EWS arm closes its ledger
the same way before it installs scopes, bumps the topology or spawns the
worker; it creates nothing server-side beforehand, so there is nothing to undo.
The arm answers with `(Option<handle>, outcomes)` and the dispatcher only turns
a handle-less answer into the whole-request `Err`. The handle's random token is
minted BEFORE the first create: `new_handle_token` is fallible (a failed host
entropy source is `Internal(RuntimeFailure)`, never retried), and a failure
there has written nothing. Minted afterwards, the same failure would return an
error over live server-side subscriptions with no handle any teardown could
name them by, orphaning them until they expire. The handle itself is assembled
after the creates because it embeds their ids (next paragraph). It stores `(server_id, expires_at)` in a
`GraphSubscriptionGroup`, and emits `Reconnected`. `push_unsubscribe`
deletes each server subscription BEFORE dropping its local state, so a failed
DELETE leaves that subscription and every not-yet-attempted sibling reachable
under the handle for a later retry. Keeping the group registered across those
awaits means "registered" no longer implies "live", so
`mark_group_tearing_down` sets a `tearing_down` marker on the group in the
same write-lock acquisition that snapshots its server ids: the renewal worker
skips condemned groups in `due_renewals` and `install_replacement` refuses
them, because a replacement created after the snapshot is one teardown can
never name - it would stay registered and delivering while
`push_unsubscribe` reported success. The marker is monotone (a later
`push_subscribe` mints a new handle) and a plain flag rather than a lock, so
neither path holds anything across a round trip. Teardown lets the renewal
worker exit once no live groups remain; condemned groups retained for a DELETE
retry do not keep it ticking. A worker that decides to exit clears its own
`graph_worker` slot BEFORE releasing the subscriptions guard that made the
decision, and `push_unsubscribe` holds that same guard across its abort: a
concurrent `push_subscribe` cannot install its live group until the slot is
empty, so its `ensure_graph_worker` always spawns a replacement. Without that
ordering a new subscription could observe a still-unfinished `JoinHandle`,
decline to spawn, and never be renewed. Lock order is subscriptions-then-worker
on every path.

Handles are self-describing so a teardown retried on a newer account instance
still works. The engine keeps a handle whose teardown failed and retries
`push_unsubscribe` on whatever instance is current; after a reopen that
instance's `graph_subscriptions` map has never seen it. A webhook handle is the
opaque string `graph2:<hex token>:<hex account>:<id>,<id>,...` carrying the
Graph subscription ids (server-global per tenant; the account's credentials can
delete them) and the hex-encoded engine `AccountId` that minted it. An orphan
whose account does not match this instance's is left alone, so a handle from
another account in the same tenant cannot delete that account's subscriptions;
the engine reopens an account under the same `AccountId`, so a genuine orphan
always matches. The unbound `graph1:<hex token>:<id>,...` shape, minted before
the binding (or when no `AccountNet` is attached), still decodes and tears
down, carrying no binding to check. An EWS handle is `ews1:<hex token>` and
carries nothing: the
streaming subscription is the worker's connection and dies with it, so an
unknown EWS handle is correctly a no-op. `push_unsubscribe` routes by the
decoded handle, not the instance's push mode (a reopen may switch modes), and
falls back to the mode for a handle of neither shape. For a handle this
instance knows, `unsubscribe_graph` keeps the registered-group path above. For
one it does not, `unsubscribe_orphan` DELETEs the ids decoded from that handle
plus the ids the renewal worker recreated under it on an earlier instance (404/410
count as gone), attempts every id even after a failure, and returns the first
failure so the engine keeps the orphan. The recreated ids live in
`RecreatedSubscriptionIds`, a ledger keyed by handle that `GraphAccountFactory`
hands to every account it opens, so it survives the reopens the engine retries
across: a recreate records its new id there under the same write lock as the
install, and on teardown an id leaves only when its DELETE succeeds, at the
same point (`forget_deleted_subscription`) where its row leaves its group. The
other exits are not teardown: a later recreate swaps the id for its own, and a
fully successful orphan teardown clears the handle's entry.

A terminal renewal failure (auth lost, permission revoked and the like) does
not remove the row: it sets `GraphSubscriptionState::terminated`. The renewal
worker skips a flagged row (`due_renewals`) and a group holding only flagged
rows does not keep the worker alive (`has_live_graph_subscription_group`), but
`push_unsubscribe` and `close()` still DELETE it. Removing the row instead
took a group's last subscription out of `close()`'s reach - only an explicit
teardown of the handle could still find it - and Graph kept delivering until
expiry (ruled 2026-10-04). Two safeguards:
ids this instance currently has registered are skipped (a stale or forged
handle cannot tear down what this instance still renews), and decoding is
all-or-nothing with an id allowlist (alphanumerics, `-`, `_`), so a malformed
handle deletes nothing and cannot inject path or query text into the DELETE
URL. An old instance's orphan retried on a new one therefore deletes the old
ids only. Known limits: the ledger is process-local, like the engine's own
subscription registry, so a handle that outlives the process (or reaches an
account built outside the factory) reaches only the ids it was minted with,
and a recreated subscription it does not name lives until Graph expires it;
and an unbound `graph1:` handle keeps the old reach, deleting its ids on any
account it is handed to.

The webhook receiver is not in this crate: consumers mount an HTTPS
endpoint at `PushEndpoint::webhook_url` and feed invalidations into the
engine `InvalidationSink`; `push_stream` carries health only.

`resource_for_scope` builds one resource per scope, and for calendars that
means `{prefix}/calendars/{native}/events` - the same calendar id
`inventory.rs` builds `calendarView/delta` from. A bare `{prefix}/events` is
the DEFAULT calendar: it both mis-targets every secondary calendar (whose
notifications would never arrive while the engine believed the scope
push-covered) and, because `subscribe_graph` groups on the resource string,
collapses every calendar scope into one subscription.

Subscription expiry is parsed with `jiff` and `parse_iso8601_to_unix` is
FALLIBLE. It drives the renewal tick, so an infallible parser that coerced an
unreadable value to the epoch reported it as long-expired and PATCHed it on
every tick indefinitely. `is_expiring_soon` still answers "renew" on a parse
failure - that is the safe direction, and one successful renewal replaces the
stored string with the expiry Graph actually granted in the PATCH response -
but it logs the bad value. There
is no hand-rolled civil-date arithmetic in the crate; `jiff` does it in both
`webhooks.rs` and `inventory.rs`.

`clientState` validation is available through
`GraphAccountFactory::with_push_endpoint_client_state(url, secret)`, which
sends the caller-owned account-wide secret on every resource's subscription
so the out-of-process receiver can compare it. There is no other webhook
constructor and no locally generated fallback: a `clientState` this crate mints
and discards is a secret nobody holds, which left the receiver with nothing to
check while the field made the subscription look validated.

#### Renewal worker

The worker wakes every 10 min, renews everything inside the 30 min
threshold, and emits `Disconnected` / `Reconnected` / `Terminated`
accordingly. `GraphSubscriptionState` retains the `CursorScope`s its resource
covers, and `due_renewals` carries them through, so a terminal failure's
`Terminated` names what lost coverage: `ErrorScope::Cursor(scope)` when the
resource covers exactly one scope, which is every ordinary case now that each
resource string is built from one folder or calendar id. `subscribe_graph` used
to discard the scopes it grouped, leaving the engine with a terminal push
failure it could not attribute to anything.

Create and renewal requests default to 10,080 minutes and clamp explicit
requests at that value. These are basic Outlook message, event and contact
subscriptions without resource data, matching the seven-day maximum in the
[Microsoft subscription resource documentation](https://learn.microsoft.com/en-us/graph/api/resources/subscription?view=graph-rest-1.0).

The health latch those events are edge-triggered off is the ACCOUNT's
(`GraphAccount::push_disconnected`), not the worker's local flag, and
`push/common.rs` owns the three transitions: `mark_push_disconnected` /
`mark_push_reconnected` publish on the edge, `announce_push_recovered`
publishes unconditionally for the one caller that knows a gap happened
without the latch ever rising (the recreate of a subscription Graph had
already dropped). `subscribe_graph` reads the same latch, so a successful
subscribe emits `Reconnected` only while push is degraded. It used to emit on
every subscribe including the first one an account ever makes, and
`Reconnected` is the engine push reconciler's full-reconcile trigger (a
`Coalesced` / `HintPayload::Unknown` invalidation over every registered
scope), so each account paid one redundant account-wide reconcile at startup.
Nothing depended on the spurious event: `SyncEngine::subscribe_push` only
records the handle and its covered scopes, and reattach re-establishes its
cursors itself - and an event published during a reattach's `push_subscribe`
races the stream forwarder's swap onto the new account anyway, so it was never
reliable gap coverage. Same shape as the EWS worker's `owes_reconnect` state
and as the google crate's fix (google-B8).

Terminal health uses a separate `watch::Sender<Option<AccountError>>`.
`publish_terminal` stores the error before broadcasting it, and
`push_stream` yields that error once and ends. This state is sticky only for
the opened account; reopening starts with an empty terminal latch.

Three further rules make the renewal path safe:

- A renewal that 404/410s (`subscription_is_gone`) is not retryable - Graph
  retains no deleted subscription to PATCH - so the worker creates a
  replacement for the same resource. The stale state stays installed until
  the replacement is in hand: dropping it first meant a failed create left
  the resource with no row at all, so no later tick could see it as due and
  coverage was lost until reopen or an explicit resubscribe. A failed
  create classifies **the create error**, not the already-known 404, so its
  recovery class decides whether another tick is worth it.
- `install_replacement` is a plain `get_mut`, never
  `entry().or_insert_with()`, and it also refuses a group marked
  `tearing_down`. The due list is a snapshot, so a concurrent
  `push_unsubscribe` can retire the handle - or condemn it and start
  deleting from its own snapshot - while the create is in flight;
  re-registering (or quietly joining) the group there would tell the caller
  teardown succeeded while notifications kept arriving. In both cases the
  worker deletes the subscription it just minted instead (best effort - the
  caller's `push_unsubscribe` may already have returned).
- A successful replacement emits `Reconnected`. The resource had NO live
  subscription between its disappearance and the create, and Graph does not
  replay notifications for that window; `Reconnected` is the only event the
  reconciler turns into a full reconcile across every registered scope, so
  without it the missed changes wait for the ordinary poll interval.

#### Cancellation safety of the webhook `push_subscribe`

`subscribe_graph` does its synchronous work inline (endpoint check, resource
grouping, handle mint, the closed-account refusal) and then hands the creates,
the rollback, the ledger close and the registration to a task spawned on the
runtime (`run_subscribe` / `create_and_register`); the caller's future only
awaits it. Dropping the caller therefore cannot skip a rollback DELETE, unwind
the create loop over live creates, or die between the `graph_subscriptions`
insert and the return. Nothing is spawned or written before the first poll, so
a future dropped before it has nothing to leak; the "guard" is the spawn, not an
object built inside the async body.

The result hand-off is two-phase: a successful `oneshot::send` is not receipt
(the value dies with a receiver that is dropped unpolled), so the waiter acks in
the same synchronous step that takes the result, and the task treats a failed
send or a missing ack as "no caller holds this handle" and retires the group
through `unsubscribe_graph`. If a DELETE fails there the group stays registered
and tearing down, so `close()` retries it. On the normal path the ack disarms
the cleanup.

`close()` interaction: `retire_all_graph_subscriptions` cancels the account
shutdown token BEFORE walking the groups, and the registration step checks the
token under the same write lock it inserts under. A subscribe that registers
first is walked by the retire; one that registers after sees the cancel, rolls
back its creates and returns an error. A subscribe on an already closed account
is refused before its first POST.

Not made safe: a create whose response never arrived (timeout, reset) may have
succeeded on Graph, but the subscription id exists only in the unread response,
so there is nothing to DELETE by and it lives until Graph's own expiry.
Recovering it would mean listing `/subscriptions` and deleting by resource and
notification URL, which can also delete another process's identical
subscription. A process exit or runtime shutdown kills the task like any other.
The same holds for the renewal recreate below.

#### Cancellation safety of the renewal recreate

The renewal worker used to run `replace_gone_subscription` inline, and `close()`
aborted the worker (as does `unsubscribe_graph` when the last group goes). An
abort landing after the create returned and before the new id was recorded, or
mid-way through the undo DELETE, stranded a live server subscription that no
group knew. Now:

- `replace_gone_subscription` spawns `create_and_install_replacement` on the
  runtime and only awaits it. The create, the install and the undo belong to a
  task the worker's abort, cancel or drop cannot reach. There is no ack
  hand-off, unlike `push_subscribe`: the install is its own effect and no
  caller holds anything that could be lost. A lost result costs only the
  `Reconnected` the worker would have announced, and the worker is only ever
  cancelled when the account is closing or the handle is being retired. A task
  panic reaches the worker as `GraphError::RuntimeFailure`.
- The install re-checks the shutdown token under the write lock it installs
  under, and `retire_all_graph_subscriptions` cancels before it walks, so a
  create racing `close()` is either installed before the cancel (the walk
  snapshots it) or refused and deleted; never neither. The group's
  `tearing_down` marker refuses the same install once the walk reaches that
  handle; the token check keeps the rule local rather than resting on the
  walk's ordering. A create that starts on an already closed account is
  skipped (`Replacement::AccountClosed`).
- A handle retired before the create returns (group gone) is the same undo:
  the fresh subscription is DELETEd, and a failed DELETE is logged only. There
  is no group left to hold the id for a retry, so it lives until Graph's
  own expiry.
- The worker is cooperative: each renewal PATCH is raced against the token
  (`biased`, so a cancelled token sends nothing), and a PATCH in flight is
  dropped, which is harmless because a renewal creates nothing. `close()` now
  JOINS the worker under `CLOSE_WORKER_JOIN_TIMEOUT` and aborts only on
  timeout. The join is what makes `close()` wait for an in-flight undo DELETE;
  the abort on timeout is safe because that DELETE lives in the spawned task.
  `unsubscribe_graph` still aborts the worker when the last group is removed,
  which is likewise safe now.

### EWS streaming mode (`PushMode::EwsStreaming`)

`push_subscribe` installs an `EwsSubscriptionState` and starts the EWS
worker. The arm itself creates nothing server-side (the worker owns the EWS
Subscribe, is never aborted except by `close()` after its bounded join, and
never races its Subscribe against cancellation, so a Subscribe whose id it read
is always released). What a dropped caller could strand is the LOCAL
registration: inserted, then parked on the worker-slot lock, handle never
delivered, streamed until `close()`. A `RegistrationGuard` is armed in the same
synchronous step as the insert (no await between them) and, if dropped armed,
spawns `unsubscribe_ews`; `subscribe_ews` disarms it just before returning the
handle. The worker: it subscribes to the union of active folders, long-polls
`GetStreamingEvents`, maps notifications to cursor scopes, and emits
`Invalidated`. Failures use `ews_error_to_account_error`
(terminal terminates; transient emits `Disconnected`, then both Subscribe
failures and broken long-polls share capped exponential reconnect backoff with
jitter).
The REST-id translation response is reconciled per requested scope. A stale or
refused folder enters the failed lane with its scope and Graph code while valid
siblings are retained in the EWS subscription. An all-refused request has no
handle, so bifrost-sync cannot record rejected scopes as push-covered.
Scope registrations ride a `watch`-channel generation counter
(`ews_topology`): the worker marks the generation seen immediately before
every read of the subscription map, so a change can never fall between a
read and a wait - a stored-permit `Notify` here turned the registration
that starts the worker into a phantom topology change and a guaranteed
redundant second subscription. A bump while the stream is live cancels the
in-flight `GetStreamingEvents`: the worker
retires the abandoned subscription with a best-effort EWS `Unsubscribe`
(Exchange holds streaming subscriptions against a per-mailbox quota, so
leaking one per scope change accrues until each times out), subscribes to
the current union, and then emits `Reconnected` - never `Disconnected`,
since nothing failed - because a change raised in the handoff window was
delivered to neither subscription and `Reconnected` is the engine's
full-reconcile trigger. Streaming subscriptions carry no resume state:
`StreamingSubscriptionRequest` admits only `FolderIds` and `EventTypes`,
so the Subscribe body is watermark-free (a `<t:Watermark>` there is a
schema violation that broke every resubscribe once a watermark had been
recorded), and gap coverage is always the `Reconnected` reconcile. The
subscribe / long-poll / unsubscribe cycle is pinned hermetically through
`EwsExecute` with a scripted transport. The worker is spawned only after a
handle has installed a scope. If a topology change leaves the union empty, it
retires the abandoned server-side subscription and exits rather than parking
for the account lifetime; a later subscribe observes the emptied slot and
starts a fresh worker.

Both push modes share one worker-slot discipline (`worker_slot.rs`): a
registration writes its state under the registration lock, drops that lock,
and only then calls `ensure_worker`; a worker that decides to exit calls
`retire_worker_slot` while it STILL holds the registration guard that made
the decision. Lock order is registration-then-worker in both modes and
`ensure_worker` takes only the worker lock, so the pair cannot deadlock.
Without the guard-held retirement, a concurrent subscribe saw a
still-unfinished `JoinHandle`, declined to spawn, and was left with no
worker once the old one returned - push silently dead. That bug appeared
independently in each mode before the lifecycle was unified.

`push_subscribe` refuses any `Folder` (public-folder) scope as
`Unsupported(PushSubscribe)` in both modes, per scope rather than per request:
one stale public folder in a mixed list used to disable push for every valid
sibling. The EWS arm narrows further via `ews_subscribable_folder_id`: only a
PRIMARY-mailbox `FolderType` scope is accepted, and a scope failing that
predicate likewise enters the failed lane alone. A non-folder scope contributes nothing to the Subscribe body's
`FolderIds` (an empty element EWS rejects outright), and a foreign folder is
addressable only with its mailbox's routing headers while Subscribe sends
`EwsHeaders::default()`, so its native id would resolve against the primary
namespace. Both are refused locally rather than turned into a remote failure
that reads as a provider fault. The predicate returns the scope's native
`restId` instead of a bool, so the check and the extraction are one step and
no later phase can re-derive the id differently. `push_stream` reads ordinary
events from a `broadcast::Receiver<WatchEvent>` and terminal health from its
watch latch, and selects against shutdown;
the EWS worker is (re-)spawned by `subscribe_ews`, whose
`ensure_ews_worker` helper starts a fresh worker whenever the slot is
empty or its task has finished.

#### REST-to-EWS id translation

A Graph folder scope carries a `restId`; EWS speaks `ewsId`, and the two are
distinct opaque formats whose only supported conversion is
`translateExchangeIds`. `subscribe_ews` therefore translates once, at the
subscription boundary, and retains the pair: `EwsSubscriptionScope { scope,
ews_folder_id }` is what `EwsSubscriptionState` stores, what
`build_subscribe_request` puts in `<t:FolderId>`, and what `scopes_for_folder`
matches a notification's parent id against to route back to the original
`CursorScope`s without a second request. Nothing downstream re-derives an id.
The mapping is many-to-one in both directions, so two pure rules sit under
it. `dedupe_by_ews_folder` collapses the union onto one entry per
`ews_folder_id` before the Subscribe body is built: overlapping
`push_subscribe` calls, or two `FolderType` scopes over one container
(translation already returns them one `ewsId`), would otherwise repeat a
`<t:FolderId>`, and whether EWS accepts, ignores, or rejects that is not
determinable here. `unique_scopes_for_folder` routes a notification to EVERY
distinct scope registered against the folder, not one arbitrary match: each
scope owns its own delta cursor, so invalidating one of a pair leaves the
other stale. Equal scopes collapse, because every repeat costs the
reconciler another full `changes_stream` run over a cursor it is already
reconciling.
Before this, Subscribe shipped `restId`s EWS cannot parse, so the whole
opt-in `with_ews_streaming()` path terminated on an opaque
`SoapFaultCode::Unknown` rather than establishing.

Three rules govern the translation call, all pinned as pure functions
because the request itself has no in-process seam:

- **Deduplicate, then chunk** (`translation_input_chunks`). A `restId` is per
  folder, not per scope, so scopes sharing a folder ask once; Graph caps
  `inputIds` at 1,000 strings and rejects the whole request above it, so a
  large mailbox fans out over several POSTs instead of failing before EWS
  setup is attempted. First-seen order is preserved, so chunk boundaries are
  deterministic. A chunk boundary is an artifact of the request size, not a
  property of the folders, so one chunk's transport failure attributes to
  THAT chunk's scopes on the failed lane (`chunk_failures`, checked in
  `reconcile_translated_ews_scopes` before the omitted-answer arm, so a failed
  POST is never reported as a contract violation) and the other chunks'
  translations stand. Failing the whole request instead discarded answers
  already collected and denied push to folders Graph had converted
  successfully. When every chunk fails nothing is left subscribable and the
  dispatcher's `Err` rule above restores the whole-request failure.
- **Per-id answers, per-id errors** (`reconcile_translated_ews_scopes`).
  Graph's `convertIdResult` reports failure PER id inside an otherwise
  successful 200: a converted id carries `targetId`, a refused one carries
  `errorDetails` and no target. `targetId` is therefore optional on the wire
  type - requiring it made one bad id fail deserialization of the whole
  response and surface as a terminal `Protocol(ParseFailed)` naming nothing.
  A refusal classifies `Request(Malformed)` -> `ClientBug` via
  `graph_error::id_translation_refused`, carrying `ErrorScope::Cursor(scope)`
  and Graph's verbatim code as a typed `WireCause::Graph`. It does not route
  through `response_to_account_error`: that needs a status, and a
  synthesized 400 on a cursor scope would classify `SyncState(CursorInvalid)`
  and send the engine to `RestartScope`, which cannot fix an id the server
  declines to convert. An omitted answer, or one with neither a target nor
  error details, is `Protocol(ContractViolation)` with the same scope. No
  arm degrades to sending the untranslated `restId`.
- **Idempotent despite the operation** (`GraphErrorContext::idempotent`). The
  call keeps `operation: PushSubscribe` - that is what the caller asked for -
  but overrides the derived idempotency. `PushSubscribe` is non-idempotent,
  so an in-flight transport drop would derive
  `Reconcile(TransportDropAfterSend)`; this read-only POST runs before any
  handle, local state, or EWS subscription exists, so there is no target to
  probe and a plain `Retry(SameRequest)` is correct. The override rides on
  `GraphErrorContext` and is applied in `finish` for builder-constructed
  errors; the `bifrost-net` transport arm re-applies it through
  `AccountError::into_builder`, since `NetErrorContext` has no override
  channel and the builder-only override does not survive that round trip.

Notification routing is an exact `ews_folder_id` byte match against the ids
this crate itself received from Graph, so a miss no longer means "the
formats may disagree". A miss still degrades to `HintPayload::Unknown`
(account-wide re-check) rather than dropping the notification.

## Mutation pipeline

`bulk_set_flags`, `bulk_move`, `bulk_destroy` share
`bulk_mutation_stream`: chunk to `batching_policy.max_items` (20), then
`submit_batch` (1) reads the batch's own ids out of the etag cache (a
per-id lookup that marks them hot, not a clone of the whole map) and
refreshes missing `SetFlags`/`Move` etags via
`GET /messages/{id}?$select=id` (`Destroy` needs none), (2) builds one `BatchRequestItem` per id - `PATCH` /
`POST .../move` / `POST .../permanentDelete` - attaching `If-Match: <entity-tag>` (mandatory
for `SetFlags`/`Move`, opportunistic for `Destroy`), (3) sends `/$batch`,
status driving `mutation_item_outcome`.

Step (2) can fail per id, and every such failure joins the chunk's outcome
list instead of aborting it: an id with no cached or refreshable etag, a
`Move` that does not resolve to a same-mailbox folder destination, and an id
whose shared mailbox is no longer configured. The last one only reaches
`request_for_mutation` on the `Destroy` path - `SetFlags` / `Move` are
filtered out one step earlier by the etag preflight, which already files
them per item - which is precisely why propagating it there was invisible
until a bulk destroy hit it. If step (2) leaves nothing routable, no
`/$batch` is sent and the chunk is answered entirely from those local
outcomes.

A failed `/$batch` envelope preserves those local outcomes and answers every
submitted id: an in-flight transport failure or an unreadable successful
response yields `Uncertain`; an unsent request or an acknowledged envelope
rejection yields `Failed`. The original classification and cause chain are
preserved with each message scope. Later chunks are still consumed, including
when the error is account-wide, so every target receives an outcome.

Destroy uses Graph's
[`permanentDelete` action](https://learn.microsoft.com/en-us/graph/api/message-permanentdelete?view=graph-rest-1.0),
never plain message `DELETE`, which moves mail to Deleted Items or recoverable storage. The permanent action
removes it from normal mailbox inventory but can retain it in Purges under
server retention policy. `delete_thread` uses the same action in its
already-in-Trash branch. That branch selects conversation members by their
`parentFolderId` and `changeKey` together, permanently deleting only those
whose folder exactly matches the resolved Trash id. It sends `If-Match` with
the etag from that same read, so a folder change before the batch rejects the
destroy. The caller's `current` container identifies the branch but is not
evidence that every conversation member is in Trash. Copies in Sent, Inbox, or
another folder survive. Threads deleted elsewhere still move to the owning
mailbox's Trash.

`bulk_set_flags` translates `FlagOp` to a PATCH body (`isRead`,
`flag.flagStatus`, sorted `categories`); `Set`
rewrites the `categories` array, `Add`/`Remove`/`Patch` touch named fields.

An op Graph cannot express is refused before the wire as
`Failed(Unsupported(UpdateFlags))`, never sent. `flag_op_is_unexpressible`
tests the BUILT BODY, not the token namespace: an incremental category op
(Graph's PATCH surface has no add/remove member operation for `categories`,
so the partial patch would drop the other tokens) and an op naming only flags
Graph does not model at all (`\answered`, `\draft`, arbitrary keywords) both
produce `{}`. Graph answers 200 to an empty PATCH, so filing it would tell the
caller a flag change landed that never reached the wire. A mixed op keeping an
expressible half still proceeds, and `Set` is full-replace so it always writes
all three owned fields.
`bulk_move` requires `MembershipScope::Folder` (else fatal pre-request).
`IdempotencyKey` is accepted but not sent (`MutationReplaySafety::None`).

### The mutation lane's terminus is the end of the stream

Every mutation batch is tagged `PageBoundary::Page`, the last one included,
and the stream closes with `Done(None)`. This is deliberate and NOT the same
gap the hydration lane had, where a stream that fired chunks tagged only
`Page` never told the engine where it terminated. `bifrost-sync`'s mutation
consumer loops on `stream.next()` and matches only `Batch` and `Terminated`;
it never reads `page_boundary` on this lane, and treats the stream ending as
the terminus. A mutation batch also carries `checkpoint: None` by
construction and advances no cursor, so there is nothing for a `Final` tag to
close - and producing one would mean buffering a chunk just to discover it is
last, buying the consumer nothing. Pinned by
`mutation_batches_are_paged_and_the_stream_end_is_the_terminus`.

## PIM primitives

Mail mutation primitives live in `pim/messages.rs`, per-message, fanning out a
`MutationTarget::Thread` via `/messages?$filter=conversationId eq ...`.
`add_to_container` is `POST /messages/{id}/move` (no symmetric remove ->
`remove_from_container` unsupported). `set_is_read` patches `isRead`;
`set_category` patches `categories[]` (reserved `$flagged`/`starred` ->
`flag.flagStatus`). `set_extended_property` patches
`singleValueExtendedProperties` when `Some`, else `DELETE`s it (404-tolerant).
All send `If-Match` if `changeKey` set.

`set_importance` patches the single-valued `importance` field in one
`If-Match`-conditioned PATCH (never clear-then-set); the read side maps it back
onto `Message.importance` (absent/unrecognized -> `Normal`).

Send / draft lifecycle is draft-backed so the trait returns an id: `POST
/messages` create, `POST /messages/{id}/send` send (`202` no-body; the Sent
Items id is rediscovered via sync/search), returning the draft id.
Inline attachments encode into `fileAttachment` JSON; standalone
`attachment_upload` is unsupported (over-limit -> `host_attachment` ->
OneDrive); `draft_update` patches mutable fields (no attachment replace).
Scheduled send PATCHes `PidTagDeferredSendTime` (`SystemTime 0x3FEF`, ISO-8601
UTC) onto the draft between create and send; the draft id is the
cancel/reschedule handle.

`SendRequest::send_as` (gated by `pim_methods.send_as`, which Graph advertises
only while `shared_clients` is non-empty, so an account with no shared mailbox
configured does not promise a path on which every request would fail; see
`reference/jmap.md` for the JMAP foreign-submission leg)
routes create/deferred-stamp/send through the shared mailbox's `shared_clients`
entry (keyed by `MailboxId`); the three helpers take an explicit
`&GraphClient`. `apply_send_as` stamps `from`/`sender`: `As` forces both to
the mailbox; `OnBehalfOf` keeps `from` = mailbox (honoring an explicit
`from`), `sender` = `user_email` (omitted when `None`). An unconfigured
mailbox is `Request(Malformed)` carrying `RequestCause::InvalidArgument`
with `field: Some("send_as.mailbox")` - the capability is present, so the
mailbox is a bad argument rather than a missing feature. The rustdoc on
`SendRequest::send_as` is the governing statement of that split.

Search uses `/messages` (`$filter`/`$search`/`$top`). It walks the primary
mailbox first, then configured shared mailboxes in sorted routing-key
order. Its opaque cursor (`SearchCursor`, `bifrost-graph-search-v1:` +
JSON) carries the current mailbox owner, that mailbox's Graph
`@odata.nextLink`, and the sorted shared-mailbox set the cursor was minted
against. The set is load-bearing, not decoration: the cursor is a position
in a WALK, so resuming it against a different set silently skips a mailbox
that now sorts before the position, or ends the walk early. A version
mismatch, a decode failure, or a set mismatch is therefore REJECTED as
`Request(Malformed)` (`ClientBug` -> `FixClientRequest`) and the caller
re-searches from page one; a search cursor holds no durable state, so a
reject costs one round trip and nothing else. There is deliberately no
compatibility arm for a bare Graph nextLink - unrecognized cursor bytes are
never re-issued as a URL. Cursor bytes are request input, so none of these
rejections is a `Protocol(...)` classification.
A shared mailbox the account has lost delegate access to must not end the
walk: the dead mailbox stays configured, so propagating its 403 as the
call's `Err` (terminal `NoPermission`) killed every retry and every
restarted search at the same position, account-wide, over one revoked
share - the exact escalation every other shared-mailbox door quarantines.
The walk instead skips that mailbox, continues into the next one in the
same call, and reports the skip as a `Page::skipped_scopes` entry
(`bifrost-types` vocabulary added for this: scope `Mailbox { id }` plus the
classified `AccountError`), so "no matches there" stays distinguishable
from "never searched there". The quarantine is exactly as narrow as
`graph_shared_scope_error`'s: only `Authorization(PermissionDenied)` on a
FOREIGN mailbox skips. A transient failure there still fails the call
(retrying the same cursor can succeed; a skip could not), and a
primary-mailbox 403 still fails terminal. The skip is a page report rather
than a `ScopeRevoked` -> `DisableScope` directive because a search walk has
no cursor scope for the engine to disable.
Message and thread ids from shared mailboxes are owner-qualified, while
primary ids remain bare; thread search dedups `conversationId` within each
page. Graph forbids `$search`+`$filter` together and
rejects `$filter` `contains()` on sender/recipient (400), so any
`SearchFilter::{From,To}` leaf (`filter_requires_search`) routes the whole
filter onto `$search`/KQL (`kql_filter`), AND-combining any `provider_query`;
`In` + From/To is `Request(Malformed)` (no KQL folder property). No-sender
searches keep the `$filter` path.

Both `pim_methods.directory_search` (non-empty lookup) and
`pim_methods.directory_enumerate` (empty-query enumeration) are true.
Directory pages default to `truncated: false`; continuation remains in
`next_cursor`.

`directory_search` (organization directory / GAL, distinct from the personal
`/contacts` corpus `contact_search` scans) queries `/users` with a
`displayName,mail,otherMails,proxyAddresses,businessPhones,companyName,jobTitle,department`
`$select`,
`$top`, and (non-empty query) tenant-root `/users` `$search`, independent
of the client's `/me` or `/users/{mailbox}` routing prefix. The expression
ORs individually quoted `displayName:q`, `givenName:q`, `surname:q`,
`mail:q` and `userPrincipalName:q` clauses with uppercase `OR` outside
the quotes. Terms escape backslash before double quote; the entire expression
is percent-encoded, with ampersands double-encoded as `%2526` for the
[documented directory-search issue](https://learn.microsoft.com/en-us/graph/known-issues#search-for-directory-objects-fails-for-encoded-ampersand--character).
Matching is tokenized word-prefix matching on displayName in any token order,
and startswith semantics on the other fields: neither substring nor domain
search. See Microsoft's
[directory search contract](https://learn.microsoft.com/en-us/graph/search-query-parameter).
Search requests carry `ConsistencyLevel: eventual`, including nextLink and
resumed pages. Empty or whitespace-only queries enumerate without `$search`
or that header. Rows without `mail` are dropped.

Only the initial, non-resumed search request can fall back: HTTP 400 with
Graph error code `Request_UnsupportedQuery` retries once using the escaped
`startswith(displayName,'q') or startswith(mail,'q')` filter. The fallback
logs status and provider code, resets the walk, results and row offset, and
mints filter-mode cursors. Other 400s, local malformed requests, and failures
on search continuations propagate without restarting. Paged directory cursors
store explicit search/filter mode alongside page URL and consumed server-row
count; the mode is never inferred from query text or URL. Older cursors without
mode retain their original filter behavior.

`additional_emails` carries `otherMails` in server order, then the SMTP half
of `proxyAddresses` in server order. A Graph proxy address is `TYPE:value`
and the type prefix is load-bearing: `SMTP:` marks the mailbox's PRIMARY
address, `smtp:` a secondary alias, and every other type (`X500:`, `SIP:`,
`EUM:`, `SPO:`) is a directory or routing identifier that is NOT an email
address and never enters the field. Both SMTP casings are kept as addresses,
and the parse PRESERVES which one it saw (only an exactly upper-case `SMTP`
counts as primary), because the de-duplication below needs that distinction
and nothing downstream can recover it. An entry with no `:` carries no type,
so its kind is unknowable and it is dropped silently, as is an empty value
after the prefix.

De-duplication runs two comparisons, because the two source lists carry
different guarantees.

- UNIVERSAL, across `mail`, `otherMails` and `proxyAddresses` alike: the
  DOMAIN half is ASCII-lowercased and the local part is compared as spelled.
  RFC 5321 s2.4 requires local-part case to be preserved and leaves its
  interpretation to the destination host, and `otherMails` is a free-form
  list that can name an external system where the local part really is
  case-sensitive.
- PROXY-ONLY, between `proxyAddresses` entries: whole-address ASCII
  case-insensitive. Exchange refuses two proxy entries differing only by
  case, so a fold there can only collapse what the provider already calls
  one address. That guarantee is about Exchange's own proxy table and does
  not extend to `otherMails`. A proxy entry that is SUPPRESSED still enters
  this set, or "de-duplicate within proxies" silently becomes "de-duplicate
  within emitted proxies" and a case variant of an entry already covered by
  `otherMails` survives.

The tie-breaker behind the split: the two error directions are not
symmetric. Folding too aggressively DROPS an address this layer cannot
reconstruct, and the consumer never learns it existed; folding too little
emits a near-duplicate, which is visible and which a consumer can collapse
using knowledge this layer does not have. Where case-sensitivity is
genuinely uncertain, both are kept.

`mail` versus the PRIMARY (`SMTP:`) proxy entry gets its own explicit ASCII
case-insensitive comparison, since the universal rule alone would leave
`mail = Ada@example.test` and `SMTP:ada@example.test` standing as two
entries for one mailbox. Only an entry that actually matches under that
comparison is suppressed: Graph documents guest accounts whose `SMTP:` entry
is not `mail`, so dropping every primary on the assumption would lose a real
address. The exception is scoped to the proxy list and does not reach back
into `otherMails` - given `mail = Ada@example.test`,
`otherMails = [ada@example.test]` and `proxyAddresses = [SMTP:ada@example.test]`,
the `otherMails` entry is kept and the proxy entry suppressed.

Folding is ASCII-only, and Unicode/IDNA equivalence is explicitly outside
the promise: ASCII folding is correct for ASCII spellings (punycode A-labels
included), general Unicode case folding is not a substitute for IDNA
processing, and half-done normalisation would turn into address loss.
Unexpected non-ASCII spellings are preserved rather than folded.

The FIRST RETAINED spelling wins, after surrounding whitespace is trimmed -
the stored value is the trimmed one, not the byte-for-byte server spelling,
and only the comparison folds. **The literal `mail` string can never appear
in `additional_emails`**; a local-part case VARIANT of it, coming from
`otherMails`, legitimately can, because under the universal rule it may be a
different mailbox.

Consent caveat, unresolved: `otherMails`, `proxyAddresses`, `businessPhones`,
`companyName`, `jobTitle` and `department` sit outside the
fixed property set `User.ReadBasic.All` grants, so a tenant consented only to
ReadBasic may answer the whole `/users` query with a 403
(`Authorization_RequestDenied`) now that they are selected, where before it
succeeded. That maps to `NoPermission` on the existing path, which is honest
but is a REGRESSION in reach for those tenants: the fix, if it bites, is a
select-narrowing retry after the first 403 rather than dropping the fields.
Not built, because it needs a live tenant to confirm the 403 actually fires
for a `$select` overreach as opposed to being tolerated with the fields
omitted. Search cursors retain
the current page URL and consumed server-row count, so an over-delivered page
resumes within that page rather than dropping matches. A tenant lacking
`User.ReadBasic.All` / `User.Read.All` 403s -> `NoPermission` (an unauthorized
directory is an error, not an empty result).

Directory groups (`account/groups.rs`, Graph-only in the workspace):
`directory_groups_list` walks `{prefix}/memberOf/microsoft.graph.group`
filtered to `mailEnabled eq true`, classifying rows into
`DirectoryGroupKind` (`Unified` in `groupTypes` -> `Unified`, else
`securityEnabled` -> `MailEnabledSecurity`, else `DistributionList`);
mail-disabled groups are dropped. `directory_group_expand` walks
`/groups/{id}/transitiveMembers/microsoft.graph.user` (nested groups
flattened and cycle-checked server-side), projecting
`DirectoryGroupMember { email, display_name }` with `mail` preferred over
`userPrincipalName`, lowercased, memberless-of-both rows dropped. Membership remains mailbox-scoped;
group expansion is tenant-root regardless of mailbox routing. Both page via bare `@odata.nextLink` bytes and need
`GroupMember.Read.All`-class consent - a harder grant than the mail
scopes; an ungranted tenant 403s -> `NoPermission` at call time (the
capability flags state protocol support, not tenant consent).

Container CRUD maps to mail folders only. `containers_list` returns
native folder ids (`Provenance { Graph, Folder, native }`), refreshes the
folder tree, and maps well-known folders; user folders stay role-less. It then
appends two namespaced legs:

- Each `shared_clients` entry's folders, `namespace = Shared`,
  `owner = MailboxId(mailbox)`, `native_id = encode_foreign(mailbox, folderId)`
  (byte-identical to the `CursorScope::FolderType` string discovery emits, so
  a container joins its sync scope by id), `owner_local_id` = the bare Graph
  folder id. The primary role map is deliberately NOT applied here - a shared
  mailbox's well-known folder ids are not the primary's. A per-mailbox listing
  failure degrades to a `SkippedScope` (naming the mailbox, carrying the
  classified error) plus the remaining containers.
- Each `routing_map` public folder, `namespace = Public`, no owner,
  `content_class` from the EWS `FolderClass`, `rights` from the EWS
  `EffectiveRights`. Purely local (reads the discovery-seeded maps, no EWS
  round-trip) and includes folders that are visible but NOT pinned for sync -
  the consumer has to see a folder before it can pin it.

The skips ride out on `ContainerList::skipped_scopes` (and are also
warn-logged), so a consumer can tell a degraded share from a deleted one.

Create/rename/move/delete call `mailFolders`; root moves target
`msgfolderroot`.

Settings are narrow: `identities_list` returns the primary `me` profile as
the default identity (`identity_update` unsupported); vacation get/set maps
to `mailboxSettings.automaticRepliesSetting`; `quota_get` unsupported.

Typed hydration is separate from `get_stream`: `message_hydrate` fetches one
message at the requested projection into `Message`; `thread_hydrate` queries
the conversation, sorted by date.

`message_hydrate` makes the SAME transport decision the batch door makes
(`pim::hydrate::ews_read_folder`, the pure discriminator `partition_ews_ids` also uses):
a public-folder id reads over EWS `GetItem` via `public_message_hydrate` +
`message_from_ews_item`, everything else over Graph REST. Without that split
the single-id door - the one a consumer reaches through the engine's hydration
passthrough - stripped the id down to its native EWS `ItemId` and asked
`/me/messages/{itemId}`, which 404s `ErrorItemNotFound` naming the bare item
id. The EWS projection keeps the folder-qualified id, maps `BodyPreview` /
HTML body onto `body_text` / `body_html`, tags the containing folder as the
only container, and carries `Importance::Normal` and no `thread_id` (EWS's
message shape reports neither).

The EWS domain kind (`EwsItemKind`) is separate from request shape
(`EwsItemShape`). Class matching requires the exact class or a dot-separated
subclass. `IPM.Note`, `IPM.Post` and `IPM.Schedule.Meeting` subclasses are Mail;
`IPM.Appointment` is Calendar; `IPM.Contact` and `IPM.DistList` are Contact.
All other classes, including unknown `IPM.*`, are Other.
Missing classes are never invented as `IPM.Note`.

The parser records the returned wire kind, including MeetingRequest,
MeetingResponse, MeetingCancellation and PostItem. Mail admission requires
both a Mail class and a message, meeting or PostItem wire kind. Posts are
the native messages of public folders; they use the common GetItem shape
because message recipient fields do not apply. Rejected parsed classes join
the operator warning, deduplicated per walk and suppressed by the cursor
after the first warning. Inventory and poll
watermarks, boundaries, live ids and version baselines use only admitted
items from the response. Enumeration uses the class-specific default shape
plus common `item:` properties, never unconditional `message:` properties.

Both hydration doors check the returned item before projection, including
`FlagsOnly` and cache hits. Non-mail is a scoped per-id
`Unsupported(Hydrate)` or `Unsupported(HydrateMessage)` refusal. No empty
Message is minted and no id silently disappears. The bounded class cache is
only a request-shape hint, refreshed by walks and pruned on emitted destroys.
Unknown items use a class-neutral GetItem request; the folder class never
substitutes for the item's domain. Public-folder hydration issues one GetItem
per id because routing headers can differ by folder.

`move_thread` calls Graph move directly (the destination is the container id
the consumer already holds, which is owner-qualified for a shared folder).
`delete_thread` resolves and caches Trash via `deletedItems` IN THE THREAD'S
OWN MAILBOX, and fails rather than guessing when that lookup does not resolve.
If the caller says the thread is already in Trash, only members whose
`parentFolderId` exactly matches that resolved Trash id are permanently
deleted; otherwise every member is moved to Trash. See the foreign-mailbox
section for why both halves of that operation are owner-scoped.
`apply_label`/`remove_label` inherit the trait default (Graph provenance ->
`set_category`, `(Folder, non-Graph)` ->
`add_to_container`/`remove_from_container`, else `Unsupported`).

## Blobs

`open_blob` decodes `BlobHandle::id` (a JSON
`GraphBlobLocator { message_id, attachment_id, kind }`), short-circuits
with a `BlobNotByteStream` warning for `Reference` kinds, fetches
`/messages/{mid}/attachments/{aid}/$value`, and emits each `bytes_stream`
chunk as a Batch. `open_blob_range` adds a `Range` header; a 200 (not 206)
to a Range request is fatal, a 405 surfaces `BlobNotByteStream` for
fallback. `blob_handle_from_graph_attachment` sets `supports_range` only
for `fileAttachment` (item/reference false); no pre-download digest.

## Error translation

`graph_error::into_account_error(error, ctx)` converts the crate-internal
`GraphError` into an `AccountError` via `try_build`.

The `GraphError::Net` arm re-decodes Graph's error envelope before
delegating. `bifrost-net` never hands a 4xx or 5xx back as a response - its
retry loop converts one into `Status` / `RateLimited` /
`RetryBudgetExhausted`, body preserved - so classifying those through
`bifrost_net::into_account_error` alone reads the HTTP status and throws the
typed `error.code` away. The whole `GraphSignal` table below then applied to
`$batch` subresponses and to nothing else: a live 400 `InvalidDeltaToken`,
the documented delta-expiry signal, classified `Request(Malformed)` ->
terminal `ClientBug` instead of `SyncState(CursorInvalid)` ->
`RestartScope`, and a 403 `AdminConsentRequired` classified
`PermissionDenied` instead of `NeedsAdminConsent`. `net_to_account_error`
refines when the body is Graph's - an envelope decoded, or there was no body
at all and the status table is the whole answer (a bare 412 is
`ConcurrencyConflict` here and an opaque server error in bifrost-net's
table). A NON-EMPTY body that is not an envelope stays on bifrost-net's
mapping on purpose: Graph did not write it, so reading it as
`Protocol(ContractViolation)` would turn an edge device's HTML 403 on the
Autodiscover leg into a terminal provider fault. `AuthLost` is never
refined - it means a 401 survived a forced refresh, stronger evidence than
the envelope, and Graph's own 401 arm derives the same recovery class.

The same transport fact governs control flow, not just classification:
`webhooks::subscription_is_gone` reads the status through
`GraphError::response_status`, which sees both the `Response` shape (`$batch`
subresponse, EWS HTTP failure, OneDrive chunk PUT) and the response evidence
a `bifrost_net::Error` preserved. Matching only `GraphError::Response` made it
permanently false on the live path, so `delete_subscription` failed on an
already-vanished row and the renewal worker never took its recreate branch.

`GraphErrorContext
{ protocol, operation, scope, idempotency_override }` threads the operation so
`recovery::derive` computes `RecoveryClass` (no private recovery table).
`idempotency_override` is `None` everywhere except the one call site that
needs it (the EWS id-translation POST, via `.idempotent()`): it corrects the
retry-vs-reconcile derivation for a read-only preflight issued under a
mutating operation, without renaming the operation the caller invoked.
`GraphError::Configuration` - the locally-detected stale-shared-mailbox
rejection, raised before any request - classifies `Request(Malformed)`
through the same `base_builder` / `finish` funnel as every other arm rather
than through `invalid_account_error`, which takes an operation and nothing
else: the rejection is always raised about a specific message, cursor scope,
or subscription resource, and dropping `ctx.scope` left an operator a bare
`request.malformed` with nothing to act on.
A provider body that does not parse is `GraphError::Json` ->
`Protocol(ParseFailed)`. The variant is not JSON-only: Graph REST JSON, the
Autodiscover XML answers and the OneDrive upload-session 202 and drive-item
bodies all raise it, so its diagnostic label says only "provider response did
not parse" and each producer's message names its own source and format (the
REST funnel prefixes "Graph REST JSON body"). A complete answer that parsed but is semantically
impossible (a repeated page link, an impossible upload offset, a refused
upload URL) is `GraphError::ContractViolation` ->
`Protocol(ContractViolation)`, both with `Acknowledged` evidence. The
client-side arms map onto the `Internal` kind and, recording no
transmission, derive `InternalFailure` (never retried): `GraphError::Internal` -> `Internal(InvariantViolated)`,
`GraphError::LimitExceeded` (a limit this crate imposes on itself, the page
budget) -> `Internal(LimitExceeded)`, `GraphError::RuntimeFailure` (a local
facility failed, the entropy source behind `new_handle`) ->
`Internal(RuntimeFailure)`.
`GraphErrorContext::ews(op)`
is the EWS constructor; `ews_error_to_account_error` routes
`EwsError::Transport` through `bifrost_net::into_account_error`, `HttpStatus`
through the REST `response_to_account_error` path, `SoapFault` onto
`SoapFaultCode` (Server -> Unavailable, Client/MustUnderstand/VersionMismatch
-> 400, Unknown -> ContractViolation), and `MalformedXml` to
`Protocol(ParseFailed)`. EWS errors stamp `Protocol::Ews`.

`EwsClient::execute` screens every 200-OK body through `check_soap_fault` then
`check_response_error` before any operation parser sees it, because EWS reports
most operation failures inside a 200 as `ResponseClass="Error"` +
`<m:ResponseCode>`. That scan is per-response-message (a later warning or
success never donates its code to an earlier error), and an error-classed
message that cannot be classified is `MalformedXml`, never success - the
operation parsers read a missing result set as an empty successful one, so
passing it through made public folders or items silently disappear.
Unclassifiable means either NO `ResponseCode` or the self-contradictory
`NoError` on a `ResponseClass="Error"` message; the two share one path. A
CLASSIFIABLE error later in the same body outranks the malformed report,
since its code carries the real classification (`ErrorAccessDenied`
quarantines just that scope).

Every EWS reader is STRICT about references and attributes. Text runs, entity
and character references, and attribute values all go through the shared
`xml_helpers` functions (`try_push_general_ref`, the strict text and attribute
readers), and one that does not resolve - an unknown entity such as `&bogus;`,
an invalid character reference, an attribute list that does not parse - is
`MalformedXml` (`Protocol(ParseFailed)`), never dropped. Dropping spliced the
text on either side into a value the server never sent: a `ResponseCode` of
`Error&bogus;AccessDenied` read as `ErrorAccessDenied` and quarantined a scope
on a code that was never sent, and a success body carrying a bad reference
passed the scan. The two whole-response scans apply it to fault text,
`ResponseCode` and `MessageText`, and unescape `ResponseClass` before comparing
it, so `&#69;rror` still reads as an error class. The per-operation parsers and
the streaming parsers in `ews_stream.rs` (subscribe and notification, whose
item, folder and subscription ids are unescaped rather than returned raw) do
the same, and there is no lenient variant left: `ews_stream.rs` uses the shared
`xml_escape` rather than a private copy.

The single-result `EwsClient::execute` scan is whole-response. It and the
single-result operation parsers are fenced by `build_soap_envelope`'s
`debug_assert!(per_answer_request_ids(body) <= 1)`: EWS answers per entry of
an `m:`-namespaced id collection, and flattening a batched response would let
one item's `ErrorItemNotFound` discard its siblings. The Subscribe folder set
is not a per-answer collection: it rides in `t:FolderIds` and receives one
subscription response for the whole set.

The typed calendar batch funnel, `EwsClient::get_calendar_items`, shares the
same HTTP/tally/SOAP-Fault path but frames and interprets each
`GetItemResponseMessage` independently in `ews/calendar.rs`. It validates the
entire document and response count before admitting outcomes. Responses are
correlated in [EWS request order](https://github.com/OfficeDev/ews-managed-api/blob/master/Core/Requests/GetItemRequest.cs), including errors without an ItemId; every
success also checks its returned native identity against the submitted id.
The single-result guard remains in place for existing callers. Public mail
hydration retains its existing single-id requests and per-folder routing.

Known Graph vocabulary lands on typed `WireCause::Graph(GraphSignal::*)` variants
(auth/access/throttle/cursor codes; see `classify`). `GraphSignal::Unknown
{ code }` is the forward-compat fallback; string-matching unknown vocabulary is
forbidden (gate-5 invariant). `ews_shared_scope_error` is the EWS twin of
`graph_shared_scope_error`: an `ErrorAccessDenied` on an owned (public-folder)
scope quarantines via `ScopeRevoked` rather than escalating account-wide.

Mapping highlights:

- `Gone` / 410 / `InvalidDeltaToken` / `SyncStateNotFound` ->
  `SyncState(CursorInvalid)` -> `Engine(RestartScope(scope))`.
- `TooManyRequests` / 429 -> `Server(RateLimited)`, `throttle_scope: Account`,
  `retry_hint: After(_)` from `Retry-After` (seconds + HTTP-date via
  `bifrost_net::parse_retry_after`), plus the Outlook mailbox
  `ThrottleBudget` wherever the request can be attributed (see "Throttle
  budgets" below); 503 / 504 -> `Server(Unavailable)` ->
  `Retry::SameRequest`.

### Throttle budgets

Microsoft documents the Outlook mail, calendar and contacts limits per app
per MAILBOX: one mailbox's throttle does not affect another. Graph therefore
never throttles tenant-wide (that paused mailboxes the provider never
throttled); `throttle_scope_for` answers `Account`, the conservative fallback,
and an attributable throttle also stamps the mailbox's `ThrottleBudget`, which
the engine then records ALONE.

- **Identity.** `GraphClient::mailbox_budget` is the API origin plus the mailbox
  the client routes to: the `/users/{key}` routing key for a shared-mailbox
  client, the profile address (`mail`, else UPN, set by
  `set_primary_address` on every primary/default-mailbox `open`) for the primary.
  Opening a derived mailbox client cannot overwrite this shared slot; derived
  clients only read it for primary-address alias resolution. Both lowercased;
  with no profile address the primary uses the engine account id, which never
  shares. It is an approximation, stated at the method: a mailbox reached
  through two address forms gets two budgets, an address reassigned while a
  deadline is pending (or a stale profile address) can hold the wrong mailbox,
  and the app id is hidden behind the `TokenSource`. Each deadline is in memory
  and expires on its own; the cost is pacing, never a misrouted request.
- **Stamp, REST funnel.** Call sites pass `Resource::Outlook`, `Batch` or
  `Unmetered` through every wrapper. Ownership is independent of URL spelling:
  continuation walks keep their starting mailbox even when Graph returns a
  quote-style `/users('id')` link. Outlook includes mail, mail folders,
  messages (including MIME drafts), calendars and events, contacts and contact
  folders, mailbox settings, inference classification, profile photos, and
  message or attachment value downloads. OneDrive (all of `cloud.rs`, including
  upload sessions, sharing links and chunk PUTs), subscriptions, profile user
  lookup (`/me` itself), directory `/users`, `/memberOf` and `/groups/{id}/transitiveMembers`
  queries and Autodiscover are
  `Unmetered`. This means no Outlook claim or mailbox throttle tag; account
  rate limits and any injected account concurrency claim still apply.
  `execute_wire` and `download_stream` wrap Outlook 429 / 503 / 507 failures
  as `GraphError::MailboxThrottled { budget, source }` using the unchanged
  `mailbox_budget` string. The budget survives classification only for
  `Server(RateLimited | QuotaExhausted)`; a plain 503 remains `Unavailable`.
  Unmetered failures and outer `$batch` failures stay untagged and fall back
  to the account key. Per-item batch stamps remain independent of the outer
  POST. `response_status` sees through the wrapper; `into_account_error`
  unwraps it into `GraphErrorContext::budget`.
- **Concurrency.** Buffered Outlook requests add a weight-1 claim on a
  `ConcurrencyLimit` of 4, Microsoft's documented concurrent-request limit
  per app per mailbox. `AccountNet::shared_concurrency_limit` shares the
  registry within one `Net`. The primary key is fixed at construction with
  an injected account or first attachment:
  `graph-outlook|ORIGIN|primary|ACCOUNT_ID`. Learning the primary address
  never changes it. Shared clients read the primary's shared address slot:
  a case-insensitive match resolves to the primary key, otherwise the key is
  `graph-outlook|ORIGIN|mailbox:LOWERCASED_ID`. ORIGIN excludes the API path,
  so v1.0 and beta share a budget. These concurrency keys are separate from
  throttle budget strings, whose identity remains unchanged.
- **Batch admission.** Each subrequest carries its owning client, resolved
  from the same object routing as its URL. The outer POST merges owners by
  final concurrency key and adds one weighted claim per merged group,
  `min(subrequest_count, 4)`. Three primary plus two shared subrequests claim
  3 and 2; six for one mailbox claim 4. Aliases are merged before capping,
  so no batch invokes the limiter's exclusive oversized-claim path.
- **Ungated streams and Exchange surfaces.** Downloads add no Outlook claim:
  a streamed body holds its permit until drained or dropped, and Graph has
  no admission deadline, so gating risks an indefinite local stall where a
  429 would be recoverable. Outlook downloads retain mailbox throttle
  attribution; OneDrive downloads are Unmetered. Downloads can therefore
  cause concurrency 429s on buffered work even when its local claims fit.
  EWS and Autodiscover are ungated. EWS throttling is per calling user under
  its own policy, and streaming notifications have a separate subscription
  budget; the REST per-mailbox 4 does not apply.
- **Concurrency residuals.** SMTP address and directory object id spellings
  of one mailbox give two keys. A shared client created before the primary
  address is known keeps its mailbox key for requests already admitted;
  later requests can resolve to the primary key. The app id is behind an
  opaque `TokenSource`, so primary identity is per Net and engine account,
  not per app. No attempt is made to infer these identities from the token.
- **Stamp, `$batch` items.** `mutation_item_outcome`,
  `reconcile_mutation_responses` and `reconcile_hydration_responses` take each
  id's budget (`GraphAccount::budget_for_object`) and stamp it on a throttled
  item.
- **Declaration.** `Account::throttle_keys` answers from the same clients:
  `budget_for_scope` follows `client_for_scope` (a bare `CursorScope::Folder` is
  a public folder read over EWS and declares nothing), `budget_for_object`
  follows `client_for_owner` (public-folder items declare nothing), and a move
  destination declares its folder owner's budget.
  `a_mailbox_declares_the_budget_its_throttles_stamp` pins that the declared and
  stamped budgets agree and that two mailboxes never share one.
- **Not attributed.** EWS (`ErrorServerBusy`), Autodiscover `ServerBusy`, the
  subscription service and webhook renewal keep the `Account` fallback, a chosen
  approximation rather than a claim about how those services meter. Renewal and
  EWS reconnect pace themselves inside this crate; their retryable errors are
  not engine lanes, so no engine hold paces them either.
- `InvalidAuthenticationToken` / 401 ->
  `Authentication(ReauthorizationRequired)` -> `AuthLost`;
  `AdminConsentRequired` -> `Authorization(AdminConsentRequired)` ->
  `NeedsAdminConsent`.
- `ConditionalAccessBlocked` / `AccessRestricted` /
  `MailboxNotEnabledForRestApi` -> `Authorization(ConditionalAccessBlocked
  | PolicyBlocked | MailboxNotLicensed)` -> `NeedsPolicyChange`.
- `AccessDenied` / `Forbidden` -> `Authorization(PermissionDenied)` ->
  `NoPermission`; `MailboxStoreUnavailable` ->
  `Authorization(MailboxUnavailable { Transient })` -> `Retry::SameRequest`;
  `PreconditionFailed` / 412 -> `ConcurrencyConflict` ->
  `Retry::AfterStateRefresh`.

`mutation_item_outcome` projects per-id `$batch` responses onto `ItemOutcome`:
2xx -> `Succeeded(Applied)`, 404-on-destroy -> `Succeeded(Skipped)`, 412 ->
`Failed(ConcurrencyConflict)`, 429/other -> `Failed` carrying `Protocol::Graph`
+ `AttemptCause(Acknowledged)` + wire signal. Before projection, bulk mutation
responses update the etag cache: flag writes record the returned etag or evict
when absent, moves and destroys evict the old id, and 412 evicts so retry must
refresh. Shared by `mutate.rs` `bulk_*`, `pim::messages::submit_write_batch`,
`get_stream`.

Cursor-decode failures (`CursorProtocolMismatch`, `CursorEnvelopeUnknown`,
`SchemaIncompatible`, and `CursorError::Decode` for cursor or page-marker bytes
that do not deserialize) build an AccountError with
`SyncState(SchemaIncompatible)`, routed to `Engine(SchemaIncompatible)`; a
scope whose shared mailbox left the configuration
(`cursor::routing_error` -> `CursorError::Configuration`) builds
`SyncState(ScopeRevoked)`. `cursor_error_to_account_error` terminates EVERY
arm through `finish`, so `ctx.scope` is stamped on all of them. That is not
just telemetry for the revoked arm: `ScopeRevoked` derives
`DisableScope(scope)` with a scope and `RestartAccount` without one, and
account-wide rediscovery cannot restore a mailbox that left the
configuration - the scope-less form put the engine into a rediscovery loop
on every pass instead of quarantining one folder.

Non-byte-stream attachments emit a `BlobNotByteStream` `Warning` rather than a
terminal error, so the engine continues past a referenceAttachment in a batch.

## An id-less delta value is a checkpoint barrier

A non-removed value in a delta page with no usable `id` cannot become an
`InventoryEntry` - there is nothing to key it by. It becomes a `Region`
obligation rather than an `Object` one, because an absent id may mean one
malformed value, a schema mismatch affecting many, a truncated page, or a
response that cannot be correlated with pagination at all, and calling it a
single-object loss would claim knowledge the walk does not have.

Its `RegionRecovery` is `CheckpointBarrier`, not `DurableReplay`. The only token
available at page granularity is the current page URL, which is a continuation of
THIS delta session: once the walk takes its `deltaLink` and the cursor advances,
that skip token is dead. Recording it as a repair capability would create durable
debt nothing could ever discharge, which is silent permanent loss wearing a
declared-debt costume. Graph publishes no durable snapshot handle for a page of a
delta enumeration, so the honest answer is that the cursor must not cross the
page at all.

The engine consequence, per `reference/sync.md`: the batch's items are still
delivered, its checkpoint travels as `PageCheckpoint::Withheld` and is not taken,
the walk stops, and a `BarrierIncident` is persisted. Earlier checkpoints from the same walk stand, because each certifies a
prefix ending before the failing page begins - the previous page's checkpoint
holds the `nextLink` that BECAME this page's URL, so accepting it does not cross
the malformed value. The scope then cannot advance until an operator waives that
specific occurrence. The obligation key names the page, not the failure class, so
a waiver cannot silently accept every future id-less value in the scope.

An earlier revision recorded the page URL as a replay token and let the delta link
advance regardless, with a test pinning that as correct.

The changes lane (`changes_stream`) enforces the same rule with the tools it
has: `SyncEvent<Change>` carries no obligation vocabulary, so an id-less
value (removed or not) in a live delta page delivers the page's decoded
siblings on a checkpoint-less batch and then terminates the stream as a
`Protocol(ContractViolation)` - the same shape as the neither-link arm -
instead of emitting the page's checkpoint. Crossing the page would advance
the cursor past a value the stream could not represent, which is silent
permanent loss. Pinned by
`an_idless_delta_value_terminates_without_crossing_the_page`.

## Bounded `nextLink` traversal

Every Graph collection walk follows server-supplied `@odata.nextLink` values
until the server stops sending them, which is an unbounded loop against a
remote: a server that keeps emitting a link spins the walk forever while the
accumulating `Vec` grows without limit. `PageWalk` guards the delta inventory
and changes walks, the contact, directory, and local event searches, and the
collection helpers including `list_mail_folders`, the `childFolders` descent,
`list_message_rules`, `calendars_list`, `address_books_list`, and
`fetch_paged_values`.

`crates/graph/src/paging.rs` holds the shared guard. `PageWalk::enter` is called
with each URL BEFORE fetching it, including the first, so a server echoing the
request URI back as its own `nextLink` is caught rather than walked. It enforces
two things, and both are load-bearing: a repeated-link check alone does not stop
a server handing out a FRESH link every page, and a page budget alone lets a
tight two-link cycle burn the whole budget on requests. `bifrost-google`'s
`calendars_list` learned the same lesson independently and carries the same
pair. The budget (10,000 pages, so 1,000,000+ objects at Graph's typical `$top`)
is deliberately generous: it bounds a misbehaving server, not a large account.

The two refusals blame different parties, and neither is a parse failure
(every page parsed). A repeated link is the provider's breach: an honest
server never hands back a link it already served, so it is
`GraphError::ContractViolation` -> `Protocol(ContractViolation)` with
`Acknowledged` evidence. An exhausted budget is not evidence against the
provider - an endless series of fresh links and an honestly enormous
collection look identical from here - so it is `GraphError::LimitExceeded` ->
`Internal(LimitExceeded)`, this crate's own limit, the same split
`bifrost-google`'s walks make. The budget is checked first, so a page past it
is the limit even when its link also repeats. Every walk, the local event
search included, classifies the `PageWalk` error through
`into_account_error` (or `graph_shared_scope_error` on the delta walks) rather
than remapping it.

`list_mail_folders_recursive` needs a SECOND, different guard, and no amount of
page-link checking substitutes for it. Folder parentage is server-supplied, so a
cycle (A claims B as a child, B claims A) or a folder reported under two parents
re-enqueues ids forever while every individual page is well-formed and no link
repeats. The descent keeps its own `expanded` set and skips an id it has already
walked. Both guards are pinned by tests that ablate to "scripted dispatch
exhausted" without them.

## Known limitations

- Discovery is mail-only (+ opt-in public folders); event/contact cursors
  are engine-constructed.
- `scope_lifecycle_stream` is empty; folder creates/renames/deletes are
  observed only at reopen. Shared-mailbox routing is otherwise complete:
  send-as (C-3), read paths and message mutations (flag/move/destroy,
  drafts) all route to the owning mailbox via the encoded message id, and
  the thread-keyed doors (`thread_hydrate`, `move_thread`, `delete_thread`,
  every `MutationTarget::Thread`) do the same via the encoded thread id.
- Public folders are poll-only (no push) and mail-only for sync.
  Non-mail folders remain visible in hierarchy metadata. The admitted-mail
  baseline is capped at 10_000 items; above it the folder is additions-only.
  With no baseline it cannot promise deletions, mail-to-non-mail retirement,
  sub-watermark non-mail-to-mail admission, in-place edits, or deduplication
  of scan additions (including timestamped additions during recovery). A scan below the cap restores tracking.
  Public contacts are available through the typed read-only contact doors;
  public calendars are available through the typed read-only calendar doors.
- EWS streaming requires EWS reachable with an accepted token; webhook mode
  needs a public HTTPS endpoint (else `Unsupported(PushSubscribe)`). EWS
  streaming currently supports primary-mailbox folders only: shared-mailbox
  scopes need mailbox-specific EWS routing headers and are not yet grouped by
  owner. Folder ids ARE translated (`restId` -> `ewsId` once at
  `push_subscribe`, retained beside the scope), so Subscribe and notification
  routing both speak the format EWS expects. Webhook mode never needed it (it
  addresses folders over Graph REST).
- Blob range per-handle (fileAttachment only); delta-token expiry reactive (410
  / 400 InvalidDeltaToken -> `RestartScope`); `MutationReplaySafety::None`.
- Unsupported: `remove_from_container`, keyword/label writes,
  standalone `attachment_upload`, `identity_update`, `quota_get`.
- Inbox rules are conjunction-shaped (`And` -> conditions, `Not` -> exceptions);
  `Or`, date ranges, remove-label, mark-unread, star, keyword, reject actions
  are rejected by local validation.

### Push failure routing

The EWS streaming worker retries locally only for `RecoveryClass::Retry`, at Subscribe, HTTP stream, and classified frame failures. Every other classification is forwarded as `WatchEvent::Terminated` so the sync reconciler can act on engine directives and reconciliation.

## Unsupported error construction

Local refusal helpers use `AccountError::unsupported` through the shared error
funnel, retaining their provider, scope and diagnostic decoration.
`Account::protocol` returns `Protocol::Graph`, including for inherited
default-method refusals.
