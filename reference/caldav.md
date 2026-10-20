# bifrost-caldav reference

Current Stage 4 standalone CalDAV account implementation.

## Public surface

- `CalDavCredentials` - Basic or bearer credentials. `bearer(token)`
  wraps a raw string; `bearer_source(Arc<dyn TokenSource>)` takes a
  shared rotation source. The bearer token is read via `current().await`
  per DAV request (inside the timed dispatcher), so a token rotated mid-sync is
  honored on the next request without reopen. A target 401 forces one
  `refresh_if_unchanged` with the exact rejected token and replays the owned
  request body using the returned token; a second 401 is `AuthLost`. Basic
  credentials are not refreshed. `Clone` only, hand-written
  `Debug` redacting the source; no `PartialEq`/`Eq`.
- `CalDavConfig` - base URL plus credentials (`Debug`/`Clone`, no
  `PartialEq`/`Eq`).
- `CalDavAccountFactory` - implements `AccountFactory`.

`CalDavAccountFactory::open(account_id)` discovers the current principal once,
then reads the calendar home, scheduling address set, and schedule outbox in
one multi-property principal PROPFIND. Discovery errors fail the open rather
than being mistaken for absent scheduling support. The factory caches the
default calendar URL and returns an `Arc<dyn Account>`
inside an `OpenedAccount` whose skip lane is always empty
(single-principal surface).
The raw DAV client, XML parser, and iCalendar projection stay
crate-private; consumers use only the factory and the shared `Account`
calendar primitives.
Discovery tries `/.well-known/caldav` first - built from the ORIGIN of the
configured base URL via `bifrost_net::url::well_known_url`, never by appending
the suffix to a configured path - and falls back to the configured
base URL only when that initial principal lookup answers that it is not a
discovery endpoint, or its successful body names no principal. Five probe
answers mean that: 404, a 405 (a static site or a proxy sitting on the origin
root in front of the DAV path, common enough that a 404-only rule failed the
open on deployments whose configured base URL works), a locally-refused
redirect - RFC 6764's canonical shape is a well-known redirecting to another
host, which the credential-origin gate cannot admit before discovery has
authenticated anything, so the walk refuses it as `Request(Malformed)` - a
redirect the walk will not follow at all (a chain past the hop cap, or a
`Location` that will not resolve or will not decode), which classifies as
`Protocol(ContractViolation)` (see `client.rs` below), and a
body that will not parse as DAV XML (`Protocol(ParseFailed)`), the motivating
case being the front end answering the PROPFIND with `200 text/html` and its
index page: the same deployment shape one status apart from the 405, and an HTML
document fails the XML parse rather than decoding as an empty multistatus. That
arm is deliberately not narrowed: ANY `Protocol(ParseFailed)` from the
well-known principal lookup triggers the fallback, INCLUDING malformed or
truncated DAV XML from a genuine discovery endpoint. The predicate cannot
distinguish the two - both arrive as an XML parse failure with no headers in
hand - so the distinction is unobtainable at this seam, not merely
unimplemented. It is safe because the fallback cannot accept bad data: it
performs a FRESH principal lookup against the configured base URL and requires
its result, then runs principal discovery normally. Nothing partially parsed
from the failed probe is reused, no origin from it is admitted, and the
base-leg and post-principal failures still propagate. The most a fallback can do
is prefer the user-configured endpoint over a probe that would not parse. The
real cost is a lost DIAGNOSTIC - an operator is not told that the well-known
endpoint is serving truncated XML - and this crate accepts that cost here rather
than fail the open on a deployment whose configured base URL works. A 401 or
403 still FAILS the open: those come from a discovery endpoint that exists and
refused the credential, and retrying the base URL would bury a reauthorization
signal. The widening applies to the well-known probe only, never to a request
against the configured base URL, where a parse failure is a real
contract violation rather than evidence that this was never a discovery
endpoint. A failure after the principal is identified is
not a root-discovery fallback trigger: a principal whose answer parses but names
no `calendar-home-set` is `Protocol(MissingField)` through the same
`missing_field_error`, which is deliberately not an arm of the fallback
predicate. `ParseFailed` says the XML was broken; `MissingField` says it was
well-formed and silent, and both derive `ProviderContractViolation`.

The `ContractViolation` arm exists because those redirect failures used to be
`Request(Malformed)` and fell back through the refused-redirect arm;
reclassifying them without widening the predicate would have silently narrowed
it. A well-known that loops is a probe answer nobody can use, the same shape as
one that will not parse, and the fresh lookup against the configured base
inherits nothing from it. On the probe path nothing else mints that kind:
redirects are disabled in the transport, so its own redirect loop never runs,
and an oversized body is mapped to `Protocol(PartialResponse)` first.

The principal walk is not this crate's. It is
`DavDispatch::discover_current_user_principal` in `bifrost-dav-core`
(`discovery.rs`), shared with `bifrost-carddav`, with
`DavProtocol::well_known_service` naming the dialect's well-known suffix. Both
legs run through one private `discover_principal(root)` inside it that performs
the PROPFIND and the decode together, so the probe's parse failure surfaces as
an `Err` the fallback predicate can read rather than being lifted past it. The
base leg never consults the predicate, and a base answer that parses but names
no principal is `Protocol(MissingField)` (built by dav-core's
`missing_field_error`, with an acknowledged attempt), while a base body that will
not parse stays `Protocol(ParseFailed)`. What stays here is the step after the principal: the
multi-property principal PROPFIND in `discover_account_for_principal`. The walk
is pinned once, for both dialects, in dav-core:
`every_fallback_answer_retries_the_configured_base` (the hop-cap loop
included), `a_probe_naming_the_principal_ends_the_walk`,
`a_refused_credential_on_the_probe_fails_the_open` and
`the_base_leg_fails_on_what_the_probe_would_have_survived`.

The DAV dispatcher bounds a complete logical request to 30 seconds, including
its credential read, one-shot 401 refresh, retries and redirect hops. Wrappers
build requests without reading credentials first; the dispatcher mints them
inside that deadline. Standalone `auth_headers` calls have the same bound. Each net
send receives only the remaining time; a new hop cannot restart the clock.
The local walk retains DAV's origin gate, dialect target checks and MOVE
Destination rebasing. A transport credential callback alone would not replace
those policies.

Status classification receives the HTTP response headers and uses
`bifrost_net::request::parse_retry_after_hint`, preserving HTTP dates as
`RetryHint::At`, including elapsed dates. Rate-limit and quota errors carry account
throttle scope as DAV's conservative scheduling policy. Exhausted retry and
rate-limit errors retain net's complete retry history, except 501, which MOVE
must read as an unsupported-method answer. A non-idempotent request can also
read its terminal status after earlier explicit 401/429/503/501 refusals;
a history containing transport ambiguity keeps reconciliation. Conditional
412 and locked 423 remain DAV conflicts; 400/422 are malformed requests and
408/502/503/504 are unavailable. Cursor-invalid 410 is interpreted only by the
sync-collection caller before the generic resource ladder.

The local multi-leg error ranking is a DAV aggregation policy. Its inputs are
multiget, filtered-query and directory-query failures. CalDAV's expired-token
error is produced only by its single sync-collection REPORT and propagates
directly, outside these folds; neither the legs nor their cursor-free net
context produce scope restart or disable directives. There is no reachable
mixed cursor-invalidation/terminal-leg case justifying a universal ordering of
engine actions. Adding a cursor-bearing leg would require explicitly preserving
its scope and action.

## Module layout

- `lib.rs` - public config / credentials / factory.
- `account.rs` - crate-private calendar-only `Account` impl.
- `client.rs` - crate-private CalDAV client: discovery, `PROPFIND`,
  `REPORT`, `GET`, `PUT`, and `DELETE`. Wire traffic rides `bifrost-net`
  through `bifrost-dav-core`'s `DavDispatch`, so DAV legs share the retry
  budget, per-host rate limiting, bandwidth metering and observability with
  every other HTTP protocol crate; scripted transcripts exercise DAV flows at
  the wire, below all of it, without a listener. DAV still mints its own
  credentials and walks its own redirects, for the reasons in "The shared
  layer" below. Every request path classifies a non-2xx status
  before the body is parsed, so an error page can never decode as an
  authoritative empty report - except the two that must read a status as data,
  and read the raw response through `report_raw_response` to do it:
  `sync_events`, which sees 403 `valid-sync-token` and 410 as cursor
  invalidation rather than failure, and the `href_query_leg` filtered lanes,
  which see a refused filter (400/405/501, or a 403 naming a filter
  precondition) as a signal to degrade to a local match rather than to fail the
  call. Both classify every other non-2xx.
  A response body exceeding the buffered ceiling is classified as
  `Protocol(PartialResponse)` with `Attempt(Acknowledged)`, so a completed
  non-idempotent mutation reconciles instead of replaying blindly.
  Credential-bearing requests are limited to the configured base origin plus
  calendar-home and scheduling origins delegated by an authenticated principal
  on that origin. Resource hrefs, consumer-provided native ids, and a
  cross-origin principal href cannot extend that internal origin set. A
  delegated origin may never weaken the transport guarantee the configured base
  URL established: when the base URL is `https`, a discovered `http` home is
  refused admission, so discovery cannot become a downgrade channel for the
  account credential. A cross-origin `https` home is admitted, because a
  principal and a calendar home on different hosts of one service is a real
  deployment shape. Discovery stages these origins without changing trust;
  they are admitted only after the complete authenticated discovery succeeds,
  before the account is shared or a home request starts. Redirects are
  disabled in the transport (`FollowRedirects::Disabled`) and EVERY hop,
  same-origin or not, is walked by `send_raw_request` in `bifrost-dav-core`:
  each hop re-mints the credential for the origin it is about to address,
  gated by the same admitted-origin set the credential gate reads, so a
  `Location` naming an unadmitted origin fails locally without a request
  going out. Walking same-origin hops too costs nothing (the credential is
  the same one) and removed the earlier split where reqwest followed those
  internally, which stripped `Authorization` on any origin change with no
  way to restore it. A 303 is not followed, and the walk is bounded by
  bifrost-net's `DEFAULT_MAX_HOPS`.
  A redirect the walk will not follow - a chain past that cap, a `Location`
  that will not resolve against the effective URL, or one that is not valid
  UTF-8 - is classified through `bifrost_net::into_account_error` exactly as
  the transport classifies its own redirect failures:
  `Protocol(ContractViolation)` with `Attempt(Acknowledged)`, deriving
  `ProviderContractViolation`. Each is raised after the server answered a hop
  and is about what it answered. They were `Request(Malformed)` -> `ClientBug`
  with no transmission evidence, and an undecodable `Location` was not an error
  at all: the 3xx came back as a response and the status ladder read it as a
  server refusal. The refused hop to an unadmitted origin is NOT among them; it
  is this client's own credential-gate decision, raised by the walk itself
  before the next hop goes out, and stays a local `Request(Malformed)` - the
  kind the discovery fallback predicate reads a refused well-known redirect by.
  It is stamped with an `Acknowledged` attempt, because the previous hop was
  sent and answered with the redirect being refused (the same stamp covers a
  move `Destination` that cannot be rebased onto an admitted origin after a
  redirect); a first-hop refusal in `auth_headers`, where nothing has been
  sent, carries none. The stamp moves no recovery class. Pinned by
  `a_redirect_the_walk_will_not_follow_is_the_providers_contract_fault` and
  `a_refused_redirect_says_the_first_hop_was_answered` in dav-core. The walk
  also gates its FIRST hop before sending, refusing an unadmitted origin as a
  local `Request(Malformed)` with no attempt: `DavRequest::new` and
  `send_raw_request` are public, so a caller can hand over a URL that never
  passed `auth_headers`. Checking only the final response, as the walk once
  did, ran after the request had already gone out.
- `parse.rs` - XML response parsers for calendar discovery, event
  listing, multiget hydration, and nested href properties. Calendar
  collection metadata and href-valued discovery properties are staged per
  `propstat` and committed only for successful 2xx statuses or a missing
  status, which RFC 4918 requires but the parser tolerates as success.
  Event listing properties (`getetag`, `getcontenttype`) are staged and
  committed by the same rule as everything else - the depth-1 listing wrote them
  straight to the committed field for a while, so an etag echoed back inside a
  refused propstat poisoned the snapshot etag that drives the change diff and
  the inventory fingerprint.
  A response with NO propstat at all still names a member, so it commits as an
  etag-less entry rather than being withheld; only a response whose ONLY
  propstat failed moves to the failure lane. `bifrost-carddav` reads it the same
  way - it used to require a successful propstat, which dropped such a response
  out of both lanes and let the diff destroy a contact that exists.
  `parse_propfind_events` returns a
  `CalDavEventListing`: committed non-collection `entries` plus `failed`
  (non-collection resources whose only propstat failed within the 207, each
  carrying its member status), so the snapshot
  diff preserves a transiently-failed resource instead of destroying it and the
  listing classifies through the RFC 4918 s13 ladder. `failed_hrefs()` projects
  that lane onto the bare ids the diff guard and `Page::failed_ids` take.
  Resource identification does not depend on an `.ics` suffix or a returned
  content type. Depth-1 listing, calendar-query, text-query, and multiget
  requests include `resourcetype` and exclude collection self-responses;
  `sync-collection` accepts every returned member href because
  that REPORT supplies neither content type nor a naming convention - the one
  response it drops is the collection's OWN, identified by href, which is also
  where the RFC 6578 truncation marker rides (see `changes_stream` below). The
  `collection` marker obeys the same commit-on-success rule as every other
  property: seen inside a `propstat` it is staged and promoted only if that
  block's status was 2xx, so a server echoing the requested prop skeleton
  (`<collection/>` included) back inside a 404 propstat cannot discard an
  event whose own properties came back 200.
  Propstat-scoped values live in one `PropStat` staging struct cleared with
  `mem::take` at commit. An absent status remains success, while a present but
  unparseable status remains an explicit non-success - including an
  empty-element `<status/>` inside a propstat, which is present rather than
  absent and therefore refuses it.
  Event listing and multiget parsers use element-stack parent checks so
  nested same-name properties do not overwrite response-level hrefs or
  propstat status. Every text-bearing parser accepts both XML text and
  CDATA. Scheduling address-set extraction returns every nested href;
  single-valued discovery properties use the first. Response hrefs are
  rebased against the URI of the request that produced the multistatus,
  yielding absolute native URLs at the decode boundary before the account
  layer can consume success or failure lanes. The base is the EFFECTIVE
  request URI: the DAV transport carries the post-redirect URL back on
  every response, because the redirect policy follows admitted-origin hops and
  RFC 4918 resolves against the URI that actually served the body.
- `ical.rs` - iCalendar projection between DAV resources and
  `bifrost-types` calendar events. Parsing-in uses `caldata`'s streaming
  `ContentLineParser` (RFC 5545 unfolding that strips exactly one fold WSP,
  and quoted-parameter splitting that tolerates `:`/`;`/`,` inside quoted
  values). Values are stored raw by caldata; text fields (summary,
  description, location, CN) are unescaped at read time via a single
  left-to-right scan. `event_from_ical` projects the ONE VEVENT an event id addresses, and
  `select_block_index` is that selection; the patch writer calls the same
  function (keyed by the event's own `recurrence.recurrence_id`), so a patch
  splices into the VEVENT the event was read from. A bare resource id is the
  master: the first VEVENT without a top-level `RECURRENCE-ID`, wherever it sits
  in the body (an override-first body patches its master, not its first block).
  A resource holding only overrides has no master: a lone override is the event
  of the bare id, and SEVERAL are `AmbiguousResource`, refused as a local
  `Request(Malformed)` naming the qualified ids, because answering with the
  first would return a VEVENT the id does not name.
  `events_from_ical` projects *every* VEVENT (master
  plus each recurrence override / CANCEL), carrying RECURRENCE-ID and STATUS
  through, and is used by the range/search listing paths (override instances
  take a recurrence-qualified `EventId` but keep the resource native id). Both
  it and `event_from_ical` mint ids through `event_id_for`, so a direct read
  produces the id the listing gave.

  **A recurrence-qualified `EventId` reads and updates the override it names;
  delete and RSVP refuse it.** `EventId("{uri}#{recurrence_id}")` is minted for
  every override the listing lanes return, so it is the only id a consumer
  holds for one - including every VEVENT of a resource with no master.
  `split_event_id` splits at the first `#`, the wire address is the resource
  alone, and the
  `RECURRENCE-ID` value selects the VEVENT by exact match against the value the
  listing minted the id from. `event_get` returns that VEVENT under the id it
  was asked for; an instance no VEVENT carries any more is `NotFound`.
  `event_update` splices only that VEVENT and PUTs the resource, leaving every
  other VEVENT byte for byte; a recurrence replacement stays `Unsupported`
  (any override present), and a `calendar_id` move is refused before any I/O
  because a MOVE relocates the whole series.
  `event_delete` and `event_rsvp` still refuse an id containing `#` via
  `reject_recurrence_instance_id`, before any I/O, as `Request(Malformed)` ->
  `ClientBug`: a URL fragment is never sent, so the request would act on the
  whole resource. Unguarded, `event_rsvp` answered for the series and
  `event_delete` DELETEd the whole `.ics` - **deleting one occurrence destroyed
  every occurrence**.
  `recurrence_instance_ids_are_refused_before_reaching_the_wire` pins the two
  refusals against an empty transport script, so a removed guard starves the
  script rather than failing quietly; the read and update halves are pinned by
  `a_listed_override_id_reads_and_updates_the_vevent_it_named`.

  The first-`#` split is right because no resource URL holds a raw `#`: it would
  be a URL fragment delimiter, so a conforming server percent-encodes a literal
  one as `%23`, which the split leaves in the resource half. A server that sends
  one raw is normalized where hrefs are resolved (`parse::resolve_href`, the one
  funnel for every href this crate reads) to `%23`; otherwise `Url::join` keeps
  it as a fragment, no request ever carries it, and the split would cut the
  resource in two. Splitting at the FIRST `#` also keeps a `RECURRENCE-ID` value
  that itself held one whole. Pinned by
  `a_raw_hash_in_an_href_resolves_as_a_literal_character` and
  `split_event_id_keeps_an_encoded_hash_in_the_resource`.

  Real per-occurrence delete would mean removing that component (an
  occurrence delete emitting `EXDATE` on the master, or `STATUS:CANCELLED` on
  the override - they differ in what attendees see). This is deliberately not
  scheduled. CalDAV is the only calendar crate with the problem, because it is
  the only one whose provider exposes no per-occurrence resource: Graph syncs
  through `calendarView`, whose occurrences carry genuine Graph ids; JMAP keeps
  overrides inside the master object and returns `Unsupported` for one it cannot
  represent; Google's `{calendar}::{event}` ids wrap the provider's own instance
  ids. None can inherit this shape, so fixing it here buys them nothing.

  A nested component this projection does not model - a vendor X-component, a
  nested VTODO - is skipped WHOLE, including any VALARM inside it. Its
  properties used to be appended to the master's own list, where
  `pick_datetime`'s specificity ladder could prefer the sub-component's
  TZID-bearing DTSTART over the event's own value.
  VALARM sub-components project into `CalendarEvent.reminders` (relative
  DURATION or absolute DATE-TIME triggers). All-day ends follow the
  exclusive `EventTime` contract: an iCalendar all-day DTEND is neither
  decremented on read nor incremented on write (it matches bifrost-google's
  verbatim exclusive end).
  Tokenizing rather than using caldata's typed builder means strict
  singleton enforcement never hard-fails ingest: a duplicate DTSTART is
  resolved with a precedence picker (VALUE=DATE > TZID > UTC > floating)
  rather than rejected. A genuinely malformed body (unterminated quoted
  parameter, missing name/value, invalid UTF-8) returns an error;
  `event_from_ical` is fallible and the listing/search paths route a
  single bad resource into `Page::failed_ids`, while the single-resource
  paths (`event_get`, `event_update` and `event_rsvp`, all through
  `fetch_event_from_url`) surface `parse_error`: `Protocol(ParseFailed)` with
  an acknowledged attempt and the event's `ErrorScope::Calendar` (the
  single-resource overload below), deriving `ProviderContractViolation`,
  because the body is the server's answer to a GET. It was `local_error`
  (`Request(Malformed)` -> `ClientBug`), which told the consumer to fix a
  request it had nothing to do with. Pinned for get and update by
  `an_unparseable_event_from_the_server_is_the_providers_fault`, which scripts
  one response per call so an update that went on to PUT would starve the
  script. A TZID-bearing local time
  projects as a bare wall-clock value
  (no false `Z`) with the zone in `timezone`; Microsoft/Windows zone names
  (e.g. `W. Europe Standard Time`) are mapped to IANA via caldata's
  proprietary-TZID table. A non-ASCII DATE or DATE-TIME grammar value is
  preserved verbatim rather than sliced at fixed byte offsets. `DTSTART`
  plus `DURATION` projects an end when `DTEND` is absent, splitting the
  duration the way RFC 5545 s3.3.6 does: the DAY and WEEK parts are nominal and
  keep their wall clock, while a time-only duration under a known TZID is added
  THROUGH the zone (same fold-earlier / gap-post-offset discipline as the rest
  of the module), so `PT10H` across a DST transition no longer lands an hour
  off. An unknown zone or a mixed duration falls back to civil addition; an explicit end
  patch removes DURATION before emitting DTEND. A resource with no VEVENT
  (a VTODO or VJOURNAL sharing the collection) maps to
  `NotFound(Calendar)` for `event_get`/`event_update` - stamped with an
  acknowledged attempt, since it is read out of the server's answer, and scoped
  to the event like the other single-resource failures - and to *no* events in
  the listing lanes, never to a fabricated empty event.
  A bare CR inside a value is one logical line to caldata's reader and is kept
  in the value, so a text field projects with the `\r` in it and a preserved
  line holding one is spliced verbatim. `escape_text` writes a CR (alone or
  before an LF) as the `\n` line break; it used to delete a lone CR, merging the
  two halves it separated when a consumer echoed the projected text back.
  **The writers refuse what they cannot write; they never strip.** Every value
  the writers emit goes through one of three treatments. TEXT (SUMMARY,
  DESCRIPTION, LOCATION, UID, VTIMEZONE TZID) is escaped by `escape_text` behind
  `text_value`, which carries CR and LF as the `\n` line break and tab as is, and
  refuses any other ASCII control character (RFC 5545 s3.3.11 excludes CONTROLs
  from TEXT; NUL has no escape form). `create_to_ical` builds the VEVENT before
  the VTIMEZONE blocks, so a zone the parameter writers refuse is named by its
  parameter field ("start timezone") rather than by the VTIMEZONE TZID. Parameter
  values (CN, TZID) are RFC 6868 caret-encoded by `escape_param` behind
  `param_value`, which carries CR, LF and DQUOTE and refuses any other control
  character. A value type with no escape form - RECUR (`RRULE`), DATE-TIME
  (`DTSTART`, `DTEND`, `RDATE`, `EXDATE`, `RECURRENCE-ID`) and CAL-ADDRESS
  (organizer and attendee addresses) - goes through `plain_value`, which refuses
  an empty value and any ASCII control character. A refusal is a `WriteError`
  naming the field (`create_to_ical` returns it, `PatchError::Write` and
  `ReplyError::Write` carry it), mapped by `write_error` to a local
  `Request(Malformed)`; a create sends nothing, an update has read the resource
  but not written it. The value is not echoed into the message. Stripping was
  rejected because it writes something the caller did not ask for; before this a
  CR or LF in an `RRULE` injected arbitrary content lines into the stored
  resource. An empty TZID (`TZID=` projects as `Some("")`) is no zone and
  writes no parameter.

  **Empty event times.** A provider projects an empty `start` for "did not
  say" and this crate's own projection does too (no `DTSTART`), and a
  consumer's read-modify-write echoes it back. No `DTEND` and no resolvable
  `DURATION` projects `CalendarEvent.end` as `None`, which a patch echoes as no
  end change; an empty end a caller passes explicitly means the same.
  An empty start is refused as malformed (`DTSTART:` is invalid and there is no
  honest reading of an event with no position); a patch that only flips
  `is_all_day` over a stored event with no DTSTART leaves it alone. An empty end
  has an honest meaning here, unlike Google, because RFC 5545 allows neither
  DTEND nor DURATION: on create it writes no DTEND (zero-length for a DATE-TIME
  start, one day for a DATE start), which reads back as no end, and
  it is not refused. On a patch it leaves the stored DTEND and DURATION untouched,
  since the empty value may stand for a DURATION the projection could not
  resolve and deleting it would destroy data. A patch cannot clear an end.
  Pinned by `an_empty_start_is_refused_on_create_and_patch`,
  `an_empty_end_on_create_writes_no_dtend` and
  `an_empty_end_on_a_patch_leaves_the_stored_end_untouched`; the value refusals
  by `create_refuses_line_breaks_in_unescapable_values`,
  `create_encodes_parameter_values_and_refuses_unencodable_ones`,
  `patch_refuses_line_breaks_in_unescapable_values`,
  `text_values_refuse_control_characters_and_still_escape_line_breaks` and, at the account,
  `unwritable_event_values_are_malformed_before_any_write`.

  Serialization-out (create/patch/RSVP) stays
  hand-rolled and verbatim-preserving: patches splice on *physical* lines,
  folding only newly emitted lines, so long preserved/unmodeled values
  round-trip byte for byte. The patch writer does not read the stored body by
  line rules of its own. `parse_vevents` records a `BlockSpan` per VEVENT - the
  caldata `LineReader` line numbers of its `END:VEVENT`, of each top-level
  property and of its first nested component, where new properties land - and `patch_to_ical` works from those spans over the
  same parse the projection ran, so a folded `END:VEVENT` or property name, a
  blank line inside a fold, a lowercase `RECURRENCE-ID`, a trimmed property name
  and a `RECURRENCE-ID` inside a nested component are read exactly as the projection read them.
  `splice_lines` groups logical lines with that same reader (a head plus every
  physical line up to the next head), emits untouched ones verbatim and folds
  only the replacements. The physical lines come from `physical_lines`, which
  splits exactly as caldata's `LineReader` does (on `\n` alone, dropping one
  `\r` before it; a bare `\r` is a break to neither, so a value holding one is a
  single logical line and a preserved `\r\r\n` ending keeps its first `\r`),
  and the pairing is VERIFIED, not assumed: every logical line rebuilt from the
  physical lines must equal what the reader produced, and the landing line must
  exist, else `splice_lines` returns `None` and the patch is refused as
  `NoSpliceableVevent` rather than spliced at a guessed line. Replacement matching is case-insensitive and
  limited to depth-zero VEVENT properties, so VALARM properties are
  preserved; new event properties are inserted before the first nested
  component. An event with no stored source body is REFUSED, never rebuilt
  from the model: a rebuild would silently drop alarms, time zones, overrides
  and every property the model does not carry, and PUT the result as a
  successful edit. `PatchError` classifies what stopped a patch:
  `RecurrenceOverride` (a recurrence replacement on a resource with override
  VEVENTs, an override being a VEVENT with a top-level `RECURRENCE-ID`) is
  `Unsupported`. The refusal still stands after the target selection above: the
  replacement would orphan the overrides when the target is the master, and
  rewrite an override's own `RECURRENCE-ID` when it is not. `NoSpliceableVevent`
  (which also covers a body whose lines the splice cannot pair with the
  reader's) and `MissingSourceBody` are
  `Internal(InvariantViolated)` with no attempt, because an event projected
  from its own body always has both. Parameter values use
  RFC 6868 caret encoding in both directions; caldata hands back raw
  parameters, so the decode of free-text parameters (CN, TZID) is this
  crate's. RFC 6868 defines no backslash escape, so a backslash is written
  and read verbatim and CN and TZID both round-trip it. The one legacy
  tolerance is `normalize_exchange_cn_param`: an unquoted `CN=Doe\, John`
  carrying a genuine `\,` / `\;` separator escape (which caldata would
  otherwise split at the comma) is resolved and re-encoded conformantly
  before tokenization. The CN read path is a pure RFC 6868 decode, which is
  what keeps a literal backslash from being eaten. The cost is that a
  display name that genuinely contains `\,` is read as Exchange's escaping;
  the two are indistinguishable on the wire. VTIMEZONE
  generation emits a single STANDARD
  block carrying the real UTC offset for the event's instant (the TZID is
  resolved against the jiff tzdb after Windows/Exchange-alias folding, and
  the offset is resolved from the DTSTART wall-clock with the same
  fold-picks-earlier / gap-takes-post-gap-offset discipline as
  ratatoskr's resolver), not the old `+0000` stub. A single block is
  approximate for a recurring event crossing a DST boundary (off by the DST
  delta on the far side) but strictly correct for the master instant. An
  unknown / unparseable zone omits the offset sub-block rather than asserting
  a misleading `+0000`.
- `capabilities.rs` - calendar-only `AccountCapabilities`.

## Account behavior

Supported calendar primitives:

- `calendars_list` - `PROPFIND` depth 1 on the discovered calendar
  home, filtering `resourcetype` entries that contain `calendar`. A home
  that is itself a calendar collection is returned by that same depth-1
  parse (its own response carries `<calendar/>`), so a home enumerating
  zero calendar collections yields an EMPTY list rather than a fabricated
  placeholder calendar. This lets a consumer distinguish a genuinely empty
  backend (and reap stale calendars) from a real single calendar, and
  avoids a phantom home-calendar whose `events_in_range` REPORT a
  spec-correct server 404s.
  A CalDAV `CalendarId` IS the resolved collection href used by the account;
  `Calendar.id`, `Calendar.native_id`, event `calendar_id`, request routing,
  and `ErrorScope::Calendar` all carry that same URL identity - with one
  documented overload: on the single-resource paths (`fetch_event_from_url`
  behind `event_get` / `event_update` / `event_rsvp`, and the dropped-create
  scope in `event_create`) `ErrorScope::Calendar` carries the EVENT's URL,
  because `bifrost-types` has no event scope and a `Reconcile(CheckTarget)`
  needs a checkable target. Ruled dav-F8 (2026-09-07): the overload is
  admitted rather than a public variant added; the reasoning is at
  `client::event_scope`.
- `events_in_range` - `calendar-query` `REPORT` with a CalDAV
  `time-range` filter, followed by local overlap filtering as a defensive
  guard. The guard applies RFC 4791 s9.9's two overlap shapes: an event with
  duration overlaps a half-open `[start, end)` window when it starts before
  `end` and ends after `start`; a zero-length instant (no DTEND, not all-day)
  overlaps when `start <= DTSTART < end`, so an instant sitting exactly on the
  window's start is IN the window (dav-F9, ruled 2026-09-07; the strict
  duration rule alone dropped it). Invalid time bounds fail locally
  before a REPORT is sent, and the encoder preserves legal one-sided ranges.
  Query REPORTs use `Depth: 1`; `calendar-multiget` REPORTs enumerate their
  hrefs in the body and use `Depth: 0`.

  **The filter runs on the server and the query asks for `getetag` ONLY.**
  Requesting `calendar-data` there is what made this lane O(collection) per
  page: the whole matching result set arrived on every page and all but
  `limit` of it was thrown away. The REPORT now answers with hrefs, those are
  sorted and sliced at the cursor watermark, and the `calendar-multiget`
  carries only the page. `QUERY_HREF_PROPS` is the prop skeleton both filtered
  lanes share.

  **A server that will not run the filter degrades rather than failing.**
  `bifrost_dav_core::filter_unsupported` decides: a `400`, a `501`, a `405`, or a
  `403`
  naming `supported-filter` / `supported-collation` / `valid-filter` /
  `supported-report` in its body. The `405` is the server refusing the REPORT
  method itself - the same answer one layer down, and the one that matters most
  for the cursor listing lane below, whose alternative would be failing a sync
  the unfiltered PROPFIND would have served. A bare `403` is deliberately NOT degraded (it
  is far more often a permission refusal, and swallowing it would replace a
  classified `NoPermission` with whatever the listing answered), and a `401`
  never is - a stale credential must reach the consumer as a reauthorize signal
  rather than as a quietly different query. The degrade lane is the depth-1
  PROPFIND: it lists the whole collection but still hydrates only the page.
  Both lanes then run through the same `hydrated_event_page`, and the local
  match is the AUTHORITY over the page either way - the server filter is a
  prefilter that may be generous, or (on the degrade lane) absent.

  Range and search results use a local WATERMARK cursor: when the page size
  truncates the page, `next_cursor` carries the RESOURCE HREF of the last
  member served, and an absent cursor is the start of the collection. The
  cursor is local and every continuation re-runs the remote request, so
  `bifrost_dav_core::sorted_candidate_hrefs` sorts (and dedups - the text
  lane's per-property REPORTs name a resource once per property it matched)
  before slicing. DAV guarantees
  no ordering on a multistatus; slicing raw response order let an unchanged
  result set come back permuted between pages, skipping the events the
  permutation moved behind the cursor and serving twice the ones it moved
  past. The watermark also holds exactly-once under a concurrent insert,
  which an integer offset did not: an event filed BEFORE the watermark
  between two pages cannot displace the unserved remainder, and one filed
  after it is served on a later page. An empty payload is a corrupt cursor,
  refused rather than read as a silent restart from the first member. A
  `limit` of zero is an exhausted page - no items and no continuation -
  rather than an empty page that names its own watermark again and loops a
  cursor-following consumer forever.

  **An OMITTED `limit` means `EVENT_PAGE_SIZE` (250) candidate resources, on both
  paging lanes.** `events_in_range` and `event_search` map `limit: None` to that
  constant, matching the CardDAV twin's `CONTACT_PAGE_SIZE`; the page carries a
  `next_cursor` whenever more candidates remain, so a consumer that omits the
  field still walks the collection a page at a time. The bound exists because an
  unbounded default would issue O(collection / `MULTIGET_BATCH_SIZE`) REPORTs and
  materialize the whole collection's projected events in memory. The page counts
  RESOURCES, and a recurring resource expands into an event per override, so a
  page can still carry more than 250 items. An EXPLICIT `limit` is honoured as
  given, however large (`usize::MAX` reaches
  `bifrost_dav_core::slice_after_watermark` and truncates nothing), and
  continuation rides `page_cursor`.

  `event_search` honours `EventSearchRequest::include_cancelled`. When it is
  `false` (the default) events whose status is `Cancelled` are dropped by a
  client-side filter on the local match, which is the authority over the page:
  the server-side text match is only an advisory prefilter, and a server that
  refuses the filter degrades the search to an unfiltered listing where a
  server-side status filter would be lost anyway. `estimated_total` stays the
  number of candidate resources the server named, an upper bound that the text
  match and this filter can only lower. A missing `STATUS` projects as
  `Confirmed`, following universal practice (RFC 5545 defines no default);
  `Unknown` means only an unrecognized value. IMAP-shaped accounts delegate
  `event_search` here unchanged, so they inherit all of this.

  **The key is the href, never the event id, and that is load-bearing.** The
  recurrence-qualified `EventId` (`{uri}#{RECURRENCE-ID}`) survives on the
  ITEMS - a time-range filter matches a recurring master whose instances fall
  in the window, `events_from_ical` still projects every VEVENT, and the local
  overlap guard still decides which instances are in range - but the cursor
  cannot key on it. The filtered lane slices before hydration and only knows
  hrefs, so keying the cursor on the event id would make a lane and its degrade
  disagree about what "already served" means: a server that started refusing
  the filter mid-walk would re-serve or skip the override instances of the
  boundary resource. The consequences are that the page size counts RESOURCES,
  so a page may carry more items than `limit` when its members expand into
  overrides and fewer when the local match rejects some, and that
  `estimated_total` is the number of candidate resources the server named -
  an upper bound on the items, not a count of them. A short or empty page that
  still carries a cursor is normal.
  The local guard uses half-open overlap, matching CalDAV time-range and the
  exclusive all-day end contract. It is recurrence-aware: a recurring master
  whose own interval sits outside the
  window is retained when its RRULE can still yield an in-window occurrence
  (dropped only when it starts after the window, or an RRULE `UNTIL` ends it
  before the window). COUNT-bounded recurrences trust the server's
  expansion: fully defending against a hostile server would require
  recurrence expansion that does not exist in this crate. Per-resource failures are not swallowed - the failed
  hrefs surface on `Page::failed_ids` so a consumer can tell a transient
  failure apart from a real remote deletion. Two kinds land there and they
  are equivalent to the consumer: a resource the server refused inside the
  207 (non-2xx propstat, or 2xx with no `calendar-data`), and one that
  fetched 200 but would not tokenize. `parse_multiget_report` returns
  `CalDavMultigetReport { events, failed, missing_data }` and reserves `Err`
  for a malformed document, so a single bad propstat can no longer abort the
  whole pull. `event_search` reports the same way, deduped across its four
  per-property REPORTs.

  Ids appear in exactly one lane. `one_outcome_per_id` drops from the failure
  lane anything that materialized in some leg, because the per-property
  REPORTs can disagree about the same resource and a consumer that saw it in
  both would count it twice and treat a displayable event as lost.

  Multiget is chunked and search runs one REPORT per property, so each REPORT
  is classified independently. A query leg that reports the filter unsupported
  degrades the WHOLE lane to the listing rather than the one property:
  answering out of the three properties a server happened to accept would
  narrow the search with no signal to the consumer. Every leg goes through `accumulate_leg`, which
  is the single funnel each leg result passes through: all four ways a leg can
  fail - transport, a non-2xx status, a body that will not parse, and a 207
  describing complete failure - are folded into `degraded` there, and the
  function returns nothing, so a leg added later has no unrouted path
  available to it. A malformed body is account-authored data, classified and
  survived rather than asserted on. A leg that fails wholly after other legs
  returned events keeps those events, and the worst recovery class
  encountered rides `MultigetFetch::degraded` into `Page::skipped_scopes` as
  an `ErrorScope::Calendar` entry - `failed_ids` carries ids with no
  classification, so folding a 401 into it would keep the data and destroy
  the reauthorize signal. A refusal with nothing usable anywhere is still an
  `Err`. That funnel governs the MULTIGET legs, which every lane now spends on
  its page, so `events_in_range` can carry a skipped scope too - it did not
  while its single REPORT both filtered and hydrated.

  The CANDIDATE leg is classified through the SAME ladder. Its 207 is read by
  the listing parser, whose failure lane carries a `FailedResource` (href plus
  the member status) rather than a bare href, so `CalDavEventListing::classify`
  runs `bifrost_dav_core::classify_207` exactly as the multiget report does: a
  207 in which every response failed, for a reason other than 404/410, is an
  `Err` with that status's recovery class, not an empty candidate set. The
  member status comes from `ResponseParts::failed_member`, which reads
  `member_status_code` - a response-level code wins, and below it only a
  response whose every propstat failed reports a failed code, chosen by the
  two-rung ladder described under "The 207 parser, collapsed": the first
  refusal that is not a 404/410, and document order only among the
  missing-property answers. Document order alone let a `404` propstat written
  ahead of a `403` one report a wholly refused collection as a benign missing
  resource, so the 207 classified `Usable` with no entries and the diff
  destroyed everything in it. The
  depth-1 listing and the snapshot poll go through the same `listing_failure`
  funnel and inherit the classification. A listing with any committed entry
  never reaches it, so a member refused beside members that answered stays a
  per-id failure on `Page::failed_ids` and the page is served; only a wholly
  refused 207 becomes an error. Pinned by
  `an_all_refused_query_207_classifies_rather_than_serving_an_empty_page` and
  `a_partly_refused_query_207_still_serves_the_page` on both sides.

  Properties are collected propstat-scoped and promoted to the response
  only by `commit_propstat`, and only from a 2xx propstat. That is what
  makes a multi-propstat response order-independent: `calendar-data`
  inside a refused block is never adopted, and a trailing non-2xx block
  for an unrelated property never retracts data a successful block
  supplied.

  The body is classified, not merely parsed. Per RFC 4918 s13 a 207 may
  describe success, partial success, or complete failure, so
  `CalDavMultigetReport::classify` returns `CompleteFailure` when no
  resource yielded usable data and at least one resource carries an actual
  non-404/410 DAV failure (a resource deleted between listing and multiget
  is the benign per-resource case). A 2xx response that omits
  `calendar-data` remains an absent-data `failed_ids` outcome but is not a
  server failure. `multiget_failure` routes a complete failure through
  `status_error`, so an all-401 body reauthorizes and an all-503 body
  retries instead of returning an empty page that a consumer would
  record as a completed walk - which would drop those resources
  permanently.
- `event_get` - direct `GET` of the event resource. Calendar id and calendar
  provenance are derived from the resource URL rather than stamped with the
  account's default calendar.
- `event_create` - creates a VEVENT resource with a UUID-backed
  `.ics` path using `PUT`, including STATUS from shared lifecycle status,
  TRANSP from shared availability, CLASS from shared visibility when
  present, ORGANIZER from the shared organizer field, and VTIMEZONE
  components for TZID-bearing start/end times. Each generated VTIMEZONE
  carries the real UTC offset for the event's instant in a single STANDARD
  block (resolved via jiff's bundled tzdb), not full timezone transition-rule
  definitions; an unknown zone emits the bare VTIMEZONE with no offset block.
  A TZID-bearing time whose value is an instant (`...Z` / numeric offset)
  renders as that instant's wall clock in the named zone (a `Z` value is
  projected through the tzdb; an unknown zone falls back to the UTC wall
  clock): emitting the UTC wall clock under a TZID would shift the event by
  the zone offset on the wire.
- `event_update` - fetches the current event, applies the shared
  `EventPatch`, and writes the replacement resource with `If-Match`
  when a strong etag was present.

  **A cross-calendar move is PERFORMED.** A `calendar_id` differing from the
  event's own collection (derived by `event_calendar_url`) relocates the
  resource; a patch that RESTATES the event's current calendar is not a move and
  takes the ordinary GET-plus-PUT path. "Differing" is decided by
  `bifrost_dav_core::same_dav_url`, which normalizes percent-encoding, host case
  and a redundant default port before comparing percent-decoded path segments -
  a byte comparison after a slash trim read a restated id in a different
  spelling as a relocation and issued a MOVE onto the collection the resource
  already lives in, which `Overwrite: F` then refused with a spurious 412. The assertion here has been inverted
  twice - the request originally returned `Ok(())` having moved nothing, was
  then refused outright as better than a silent drop, and is now carried out -
  so `event_update_moves_across_calendars_and_updates_in_place_otherwise` pins
  both halves.

  WebDAV `MOVE` is the atomic form and is tried first, with the resource's own
  file name at the destination and `Overwrite: F`, so a collision refuses rather
  than destroying a stranger's resource. `Destination` is credential-gated
  against the same admitted-origin set as the source, so a consumer-supplied
    `CalendarId` cannot steer a write anywhere the gate would refuse. A redirect of
  the SOURCE rebases `Destination` onto the same hop, inside the credential gate
  and re-checked against `is_trusted_url` after the rebase: the destination is
  made relative to the pre-hop URL and joined onto the post-hop one, so a
  cross-collection move stays cross-collection in the new namespace, and a
  destination on a different origin than the source is left verbatim. Only 405 and
  501 mean "no MOVE support"; 412 (destination occupied) and 502 (destination
  refused) stay real errors.

  A server without MOVE falls back to PUT-to-new then DELETE-from-old. That
  order is the recoverable one: a failed copy leaves the event exactly where it
  was, while a failed delete leaves it readable in two places. The delete-leg
  failure is wrapped `Protocol(PartialResponse)` with an acknowledged `Attempt`
  (via `partial_sequence_error`, which `event_rsvp` now shares), so a consumer
  can tell "not moved" from "copied but not cleaned up" and reconciles instead
  of replaying a write that already landed.

  A move-only patch is ONE request beyond the fetch - no content changed, so no
  write is issued and no partial-failure verdict can attach to a leg that was
  never needed. Whether content changed is decided by zeroing `calendar_id` on
  the patch and comparing against `EventPatch::default()`, not by comparing
  serialized bytes: the writers re-emit, so a byte comparison reads a move-only
  patch as a content change. Deriving it from the patch also means a field added
  to `EventPatch` is covered automatically.

  **The event id changes, and `event_update` cannot report it.** A CalDAV
  `EventId` IS the resource URL, and the trait method returns `()`. The consumer
  learns the new id through sync, as a destroy plus a create. This matches
  `bifrost-google`, whose `events.move` inside `event_update` has the same
  property for the same reason; reporting it would reshape a published
  signature. Weak ETags retain their `W/` marker for
  snapshot comparison but deliberately make the PUT unconditional because
  If-Match requires strong comparison (RFC 7232), so a weak validator has
  no conforming conditional form. Against a server that only ever emits
  weak ETags this means `event_update` has no lost-update protection at
  all; no better option exists inside HTTP, and a consumer that needs the
  guarantee needs an application-level revision check. When the current resource carried raw
  iCalendar data, updates rewrite each VEVENT property the patch carries -
  summary, description, location, start/end, status, transparency
  (TRANSP), class (CLASS), recurrence, and attendees - while preserving
  untouched properties such as organizer. A `visibility` patch that
  resolves to `Default` strips any existing CLASS rather than writing one,
  matching the create path. Non-VEVENT components such as
  VTIMEZONE are preserved on raw-backed updates. Multi-VEVENT recurrence
  override components are preserved for scalar patches, but recurrence
  replacement is rejected as `Unsupported` for resources with override VEVENTs
  because the shared recurrence model cannot rewrite those instances
  losslessly.
- `event_delete` - deletes the DAV resource. A target already gone (404 /
  410) is a success, and the DELETE replays after a mid-flight drop; see
  "DELETE is replayable" below.
- `event_rsvp` - uses an email-like Basic username, or a mailto address
  discovered from the principal's `calendar-user-address-set`, to rewrite
  the matching attendee's participation status. When the principal
  exposes `schedule-outbox-URL`, RSVP first posts an iTIP `METHOD:REPLY`
  to that outbox - with the RFC 6638 `Originator` (the replying user's
  calendar address) and `Recipient` (the organizer's address) headers,
  each normalized to a `mailto:` URI when bare - and then applies the same
  raw-preserving replacement path to the local resource. Accounts without
  an identifiable attendee, organizer, or schedule outbox return
  `Unsupported`. The advertised `pim_methods.event_rsvp` flag is
  discovery-derived, not assumed: `scheduling_available` requires BOTH a
  `CALDAV:schedule-outbox-URL` on the current-user-principal AND a
  calendar-user-address for this user (from
  `CALDAV:calendar-user-address-set`, or configured explicitly). A plain
  RFC 4791 store that advertises neither reports `event_rsvp = false`, so
  a consumer's capability gate rejects the call up front instead of the
  iTIP POST failing on the wire. The address-set is treated as a set:
  discovery examines every href and uses the first case-insensitive
  `mailto:` URI.
  The outbox POST and local-resource PUT remain two server operations. Every
  failure after the POST succeeds - the PUT itself and the local patch and
  iCalendar encoding steps between them - is wrapped as a
  `Protocol(PartialResponse)` carrying an acknowledged `Attempt` plus the
  original cause chain. This routes the non-idempotent RSVP to reconciliation
  and tells the consumer that the organizer may already have received it. The
  flow is still not atomic; the error class is what communicates that.
- `event_search` / `event_autocomplete` - non-empty searches issue
  CalDAV text-match `calendar-query` `REPORT`s over VEVENT summary,
  description, location, and attendee, asking for `getetag` only. The union of
  the four legs is a PREFILTER over candidate hrefs; `event_matches` over the
  hydrated page is the authority on what the page finally carries. The two
  are not the same predicate - the local match reads PROJECTED fields and
  folds case with Rust's full Unicode rules, where `i;unicode-casemap` sees
  four raw properties - and the decision is to keep the server side wide and
  narrow locally, because the reverse direction drops matches silently.

  Empty search lists the collection to preserve match-all behavior, and a
  server that refuses the text filter degrades to the same listing. All three
  sources - the time-range query, the text query, and the depth-1 listing -
  produce the same `HrefQuery` and run through the same `hydrated_event_page`,
  which is what keeps one cursor valid across a mid-walk degrade. Paging a
  large calendar therefore never costs a hydration of anything but the page.
  `Page::estimated_total` is the candidate count, and `failed_ids` carries
  both legs - the candidate leg's refused hrefs (collection-wide, and
  re-observed on every page per the `Page::failed_ids` contract) and the ones
  this page's own multiget lost. A watermark past every href is an empty final
  page that spends no multiget at all.

Cursor support is calendar-event only. `discover_cursor_scopes` returns one
`CursorScope::Folder(FolderId(collection_href))` per discovered calendar, and
NOTHING when the calendar home holds no collections - the collection walk
already returns the home itself when the home is a calendar, so an empty walk
is an empty backend and fabricating a home scope would point every cursor and
inventory request at a 404.
`establish_initial_cursor`
builds a hybrid cursor from the calendar URL, the collection
`sync-token` when present, and a sorted href/etag snapshot.
`changes_stream` uses WebDAV `sync-collection` when the cursor carries a
sync token, issuing the REPORT with `Depth: 0` as required by RFC 6578,
and applies returned href/etag/status entries to the snapshot
(deleting only on explicit per-entry `404`/`410`), and emits
created/updated/destroyed event changes. Each report builds a hash index of
normalized resource keys once from the snapshot, then maintains it as members
are inserted or removed. Exact href spellings take priority; equivalent URL
spellings reuse the first sorted existing native id without rewriting published
ids. New hrefs therefore require a hash lookup instead of a snapshot-wide URL
comparison scan. Keys follow `same_dav_url`, including segment-wise percent
decoding, host case, default ports, query identity and trailing-slash handling.
The sorted snapshot still incurs vector shifts on insertion and removal.
A per-member status that is neither
2xx nor `404`/`410` is the server declining to report on that member (403, 507,
503): the prior entry is PRESERVED untouched rather than upserted, because an
upsert records an etag-less entry and emits a change for a resource nobody
observed - and the etag-less entry then makes the next poll report an update as
well. The member status itself comes from
`ResponseParts::member_status_code`: a response-level status wins (RFC 6578
reports a removed member as a response carrying its own `404`/`410` and no
propstat); below that, any successful propstat makes the member successful,
and only a response whose every propstat failed reports a failed code - the
first refusal that is not a 404/410, falling back to document order when every
refusal was a missing-property answer.
A propstat `404` is a per-PROPERTY miss that servers answer for any requested
property they lack, beside the `200` propstat carrying the etag; reading it as
the member status destroyed a live resource.
The collection's OWN response is removed before the entries reach the snapshot.
The REPORT requests only `getetag`, so nothing but the href distinguishes it
from a member, and left in place it becomes a snapshot entry for the collection
plus a phantom `Created`. Its status is also the RFC 6578 s3.6 truncation
marker: a `507` there says the server returned only part of the change set and
the accompanying sync-token records only that partial progress. `changes_stream`
DRAINS a truncated result, re-issuing the REPORT with each returned token until
one comes back untruncated, bounded by `SYNC_TRUNCATION_ROUNDS` and stopped
early if a server claims truncation while handing back the same token. Without
the drain the partial token was checkpointed as complete and every change past
the truncation point was lost until etag drift happened to surface it; iCloud
and large Cyrus collections truncate in practice. Calendars without a sync token
fall back to polling snapshot diffs. The PROPFIND-snapshot diff (not the
sync-token path) is hardened against destroy-everything failure modes: an
empty multistatus against a populated prior snapshot suppresses the
mass-delete, and any href in `current.failed_hrefs` is preserved rather
than destroyed. The checkpoint preserves the prior entries and etags for
both no-observation shapes while retaining the prior collection token.
The shared snapshot helper returns an `Observation`; only complete observations
advance tokens. Initial snapshots with refused members carry no token, so the
next poll repeats the listing. Incremental reports with refused members retain
the last token before the refusal and stop draining, so the next poll retries
the missing ground. Equivalent member URL spellings in sync reports match the
existing entry and retain its native id for updates and deletions.
This deliberately means a genuine delete-all is not reported on that poll or a
later poll: the empty success is indistinguishable from the transient empty
response the guard suppresses.
`inventory_stream` emits event inventory entries with ETag fingerprints for
the same cursor scope and reports full coverage of that one collection. A
listing with refused members fails the inventory before emitting entries,
coverage or a checkpoint, using the member status classification. Legacy
`CursorScope::Type(CalendarEvent)` calls remain accepted for compatibility,
target the default calendar, and report only a `caldav` provider region rather
than falsely claiming whole-type coverage.
The CalDAV cursor payload is version 2. Version 2 records the request-relative
native-id namespace; version 1 cursors are rejected so a namespace correction
cannot surface as a delete plus create during snapshot diffing. The rejection is
classified `SyncState(SchemaIncompatible)`, deriving to
`Engine(SchemaIncompatible)` rather than a scope restart: only that directive
also deletes the backfill checkpoint, and without the re-walk the objects
already backfilled would keep their pre-correction id spelling.
Folder-scoped cursors must name an equivalent collection URL in their payload;
a mismatch is rejected as schema-incompatible before polling. Legacy type
scopes have no collection URL to compare.
An expired token reported as 403 with `DAV:valid-sync-token`, or as 410,
becomes `SyncState(CursorInvalid)` scoped to the invalid calendar's own
folder cursor (`ErrorScope::Cursor(CursorScope::Folder(collection url))`),
which directs the engine to restart exactly that per-calendar cursor. The
scope must be the folder scope: live cursors are one per calendar, so a
type-wide scope would ask the engine to restart a cursor that does not
exist while the stale one failed identically on every poll. Cursor entry counts are payload-bounded
before allocation. Range, search, inventory, changes, and their failed-id
lanes all use resolved absolute resource URLs as native ids. Snapshot-poll
fallback refreshes a collection token with a depth-0 `sync-token` PROPFIND,
rather than repeating the calendar-home depth-1 listing.
When a successful `sync-collection` response omits its required replacement
sync token, the account emits a warning and retains the previous token. This
keeps replay-and-deduplicate behavior while exposing the server violation.

**The cursor listing is VEVENT-filtered on the server.** A calendar collection
may hold VTODO and VJOURNAL resources beside its events - RFC 4791 leaves that
to `supported-calendar-component-set`, which many servers never restrict - and
the depth-1 PROPFIND carries no component type, so a task resource used to enter
the event snapshot and be emitted as a created event change that hydrated to
nothing. `client.list_event_hrefs_filtered` is what the snapshot lanes
(`establish_initial_cursor`, `inventory_stream`, and the polling `changes_stream`
fallback) now take: a `calendar-query` REPORT carrying the bare
`comp-filter` VEVENT and no time-range, asking for `getetag` only. It is the same
`calendar_query_body` the range lane uses with both bounds absent, returns the
same `CalDavEventListing` with the same failure lane, and costs the same single
round trip the PROPFIND did.

A server that will not run the filter degrades to the unfiltered depth-1
PROPFIND, which is what these lanes did unconditionally before. The degrade is
not optional: a narrower listing is only safe when the server actually applied
the predicate, and an event silently dropped here would be reported to the
consumer as a deletion by the snapshot diff. `filter_unsupported` degrades a
`405` for exactly this lane - a store that refuses REPORT would otherwise fail
every `establish_initial_cursor` outright, which is strictly worse than carrying
the odd task resource. Both halves are pinned by
`the_cursor_listing_asks_the_server_for_vevent_resources_only` and
`the_cursor_listing_degrades_to_the_propfind_when_the_filter_is_refused`, whose
assertions read the REQUEST, so a lane that reverts to the PROPFIND fails rather
than passing on an unchanged snapshot.

**The unfilterable lanes read the content type as component evidence (caldav-F1
/ F2 / dav-F10, ruled 2026-09-07).** Two lanes cannot carry the VEVENT
`comp-filter`: the `sync-collection` REPORT, because RFC 6578 has no filter
grammar, and the depth-1 PROPFIND the cursor listing degrades to. Both ask for
`getcontenttype`, and RFC 4791 s5.2.6 lets a server answer
`text/calendar; component=vtodo` (SabreDAV and Baikal do). `parse::declared_non_vevent`
reads that parameter, in the POSITIVE only: a `component=` naming anything but
VEVENT drops the member from the degrade listing before it reaches the snapshot,
and marks the sync entry (`CalDavSyncEntry::non_vevent`) so `apply_sync_report`
treats it as absent - never inserted, and removed with a `Destroyed` if an
earlier poll let it in as a phantom. A bare `text/calendar` or a missing content
type is not evidence and admits the member exactly as before. The other shapes
were considered and not taken: a comp-filter query run after the sync report
cannot tell "not a VEVENT" from "created after the query ran", a
listing-plus-filter pair costs two round trips on every creating poll, and a
per-href classification mark in the cursor is a version-3 envelope. Pinned by
`a_declared_component_marks_non_vevent_members_on_both_unfiltered_lanes` and
`a_declared_task_is_dropped_from_the_sync_snapshot`.

Residual, now narrowed to servers that emit NO `component=` parameter: on those,
a VTODO created or modified between two token polls is still reported once as a
created/updated event change that hydrates to nothing, and then sits in the
snapshot; nothing establishes or re-walks with it, so the leak is bounded to
resources touched during a token-backed poll. The degrade lane has the matching
cost on the same servers: a transient `405` on the filtered REPORT (read by
`filter_unsupported` as "the server will not run the filter") walks the
unfiltered PROPFIND, puts the collection's tasks into the snapshot as `Created`,
and the next successful filtered REPORT reports them `Destroyed`. That flap is
the accepted price of a degrade that must not drop real events; it does not
occur where the content type labels the task.

All mail, contact, filter, blob, push, and settings methods return
`AccountErrorKind::Unsupported` stamped with `Protocol::CalDav`.
`set_priority` and `set_bandwidth_cap` forward to `client.net()`, the crate's
`AccountNet` handle, so DAV traffic is scheduled and metered like every other
HTTP protocol crate's. A CalDAV leg composed into an IMAP-shaped account is
handed that account's own `AccountNet` at construction, so it shares the
account's priority, bandwidth meter and cap rather than running unmetered
beside them.

## Sync scope is per calendar collection

All three sync lanes resolve their collection from the folder scope returned by
discovery. An account with three calendars therefore has three independent
cursors, inventories, and change streams. The default calendar URL remains the
fallback for PIM calls that omit a calendar and for legacy type-scoped cursor
calls; it no longer limits discovered sync coverage. Standalone and
IMAP-composed opens consequently have no collection-limit skip entries.

The default is `Option<String>`, and it is `None` when the calendar home
enumerated no collections. It is deliberately NOT the calendar home in that
case: `list_calendars` already returns the home when the home is genuinely a
calendar collection, so an empty walk means an empty backend, and addressing the
home would send every collection-less call to a resource a spec-correct server
404s - reporting a local routing failure as a remote `NotFound`, and
contradicting the empty-home contract `calendars_list` is pinned to. A call that
names no calendar against such an account fails locally with
`Request(Malformed)` -> `ClientBug` before any I/O.

Only the doors taking an `Option<CalendarId>` reach the default at all -
`event_search`, `event_create` and the legacy type-scoped cursor calls.
`event_get`, `event_update` and `event_rsvp` derive the collection from the
resource's own URL via `bifrost_net::url::parent_collection_url`, which answers
for every id that resolves absolute, so their fallback to the default is
unreachable in practice and kept only as a total match arm.

`open` delegates to `open_with_client` so the whole discovery-to-account path
can be driven against a scripted transport; `open` itself builds its client from
a `CalDavConfig` and so cannot take one. Both halves are pinned - the selection
by `an_empty_home_leaves_no_default_calendar`, and the opened account by
`an_empty_discovery_opens_an_account_with_no_default_calendar`, which is the one
that bites if the home fallback is reintroduced at the call site.

Multi-leg calendar multiget and text-search REPORTs are dispatched
concurrently, bounded to `MULTIGET_LEG_CONCURRENCY` in-flight legs. The bound
is a per-call local policy bounding one input-sized fan-out. No account-wide
DAV bound exists: no server advertises one and nothing needs one yet.
`bifrost-net::ConcurrencyLimit` is available if one is ever wanted. Dispatch
is ordered, so the
merged report and the surviving degraded error do not depend on completion
order. Offset continuations still re-run search so each page reflects a
fresh server observation and carries that observation's failure lanes.

## The shared layer: bifrost-dav-core

The protocol-neutral half of this crate lives in `bifrost-dav-core`, a PRIVATE
shared crate on the `bifrost-sasl` precedent. Nothing published moved:
`CalDavConfig`, `CalDavCredentials`, `CalDavAccountFactory` and the `Account`
impl are exactly what they were, and consumers see no change.

What moved: `DavRequest` / `DavResponse` / `DavBody`, `settle_body`,
`url_origin`, `origin_is_secure`, the etag helpers (`response_etag`,
`normalize_http_etag`, `prepare_if_match`) and `PutCondition`, the whole
status-to-`AccountError` ladder with its constructors, and
`worse_recovery` / `recovery_rank`.

`status_error` and `parse_error` stamp an `Attempt(Acknowledged)`. Every status
the ladder classifies is one the server sent - on the response line, or as the
member status of a 207 whose every response failed - and a body can fail to
parse only because the server answered one. It is the same transmission
evidence `bifrost-net` stamps on the status-bearing failures it classifies
itself. On the status ladder it moves no recovery class (`derive` treats an
acknowledged attempt like an unsent one on every kind that ladder mints); its
absence read as `Unsent` in telemetry and support exports, claiming the request
never left the process. `local_error` carries no attempt and `transport_error`
an `Unsent` one. Pinned by
`a_server_answer_carries_acknowledged_transmission_evidence`, which also
asserts that an answered 503 on a non-idempotent create still retries and a 404
is still `ProviderRefused`.

Then the `current-user-principal` walk, as
`DavDispatch::discover_current_user_principal` in `discovery.rs`, described at
the top of this document. Each crate keeps only the step after the principal.

Then the whole request dispatcher, as `DavDispatch`: the `AccountNet` handle,
the credential store, the admitted-origin set, the credential gate
(`auth_headers` / `is_trusted_url` / `admit_discovered_urls`), `resolve_url`,
the redirect walk in `send_raw_request`, and the generic verbs `propfind_raw`, `report_raw`, `report_raw_response`,
`delete_resource` and `move_resource`. `CalDavClient` is now a newtype over it
plus the CalDAV-specific bodies and parsers. This was the security-sensitive
half: the credential gate, the HTTPS-downgrade refusal and the redirect walk
were all duplicated, and a divergence there is a credential leak. Ablating
`is_trusted_url` in the shared crate fails three tests in each crate.

**How `resolve_url` turns a native id into a request URL.** Three shapes, and
only the third is interesting. An href already carrying an `http://` or
`https://` scheme is returned verbatim - that is what makes a recurrence-qualified
`EventId` resolve to its master resource, and what a Multi-Status naming an
absolute href means. A ROOT-RELATIVE href (`/cal/x`) replaces the base URL's
path, per RFC 3986, which is deliberately unchanged and is what a server naming
such an href intends. A RELATIVE href (`cal/x`) lands UNDER the configured base
path.

That last case is why `join_base` restores the trailing slash `around()` trimmed
before joining: `Url::join` reads a base whose path does not end in one as
naming a RESOURCE, so `https://host/dav` + `cal/one.ics` resolved to
`https://host/cal/one.ics`, silently relocating the id out of the collection the
account lives in. Appending is also what the unparseable-base string fallback in
the same function has always done, so the two branches agree on the shape they
share. The reason lives at `DavDispatch::resolve_url` and `join_base` in
`crates/dav-core/src/dispatch.rs`; this is the contract, not a second copy of
the argument.

`DavRequest::header` is a fluent builder, so it cannot return a `Result`. A name
or value the HTTP grammar rejects is therefore RECORDED on the request
(`invalid_header`, exposed by the accessor of the same name) and raised by
`dispatch_once` as a local `Request(Malformed)` before any I/O; the header copy
into the `bifrost-net` builder, which takes `&str`, refuses a non-ASCII
`HeaderValue` the same way. Both used to be silently dropped, which put a request
on the wire MISSING a header the caller asked for - a conditional PUT demoted to
an unconditional one loses its lost-update guard without telling anyone. The
credential half of the same defect was fixed earlier, in `auth_headers`. Pinned
by `an_invalid_header_value_is_recorded_rather_than_dropped` in dav-core and, at
the wire, by `a_header_the_record_could_not_carry_fails_before_the_wire` and
`a_non_ascii_header_value_fails_before_the_wire` in bifrost-carddav, both of
which script an EMPTY transport so a regression panics on an exhausted script.

`CalDavCredentials` stays published and unchanged; `to_shared` projects it onto
the dispatcher's `DavCredentials`, cloning the `Arc` so a bearer token is still
read live from the shared source at every request.

`DavDispatch` also supports an optional `DavRequestGuard` installed before
sharing an account. The dispatcher invokes it before each hop, for the request
URL and any Destination header after redirect rebasing. CardDAV uses this hook
to refuse every method and operation inside its monotone directory set except
a DirectorySearch REPORT to an exact admitted search gateway. This includes
pre-fetch GETs and sync requests, redirected targets and rebased destinations.
Later directory markers tighten the set without widening origin trust or
search capabilities; CardDAV sync boundaries retire affected original scopes
through ScopeRevoked / DisableScope.
CalDAV installs no guard and retains its existing origin and redirect policy.

**Which requests are declared unreplayable, and why it is a request-level
declaration.** `DavRequest::idempotent(false)` is set at two call sites: the
create-PUT (`If-None-Match: *`) in both crates' `put_event` / `put_vcard` - which
is also the copy leg of the MOVE fallback, one caller up, so the two lanes share
one declaration - and the `MOVE` in `DavDispatch::move_resource`. Each is a
request whose blind replay after a mid-flight drop misreports a mutation that
SUCCEEDED: a replayed create answers 412 and the consumer re-creates under a
fresh UUID, leaving two copies; a re-issued update after a committed MOVE
answers 404 at a source the first attempt already emptied, which the status
ladder reads as `NotFound` -> `ProviderRefused`. `If-Match` and unconditional
PUTs are deliberately NOT declared: they address a known URL with absolute
state, so a replay lands on the same end state, and a replay arriving after the
first attempt committed answers 412 -> `ConcurrencyConflict` ->
`Retry(AfterStateRefresh)`, which is already the right handling. Losing that
resilience on the update lane is a cost the fix must not impose.

**DELETE is replayable, and a gone target is a success (dav-D1, ruled
2026-09-07).** The DELETE was briefly declared unreplayable too, because a
replayed DELETE that had landed answered 404 and the ladder reported a
successful delete as `ProviderRefused`. That traded away delete resilience: every
drop, whether the request landed or not, reached the consumer as
`Reconcile(CheckTarget)` and cost a probe. The ruling takes the other answer.
`DavDispatch::delete_resource`, `delete_event` and `delete_vcard` absorb a 404
or 410 (`delete_target_already_gone`) as success under `reference/error-model.md`'s
"a benign NotFound lives at the call site" rule - a delete whose target is gone
has reached its intended end state - and the DELETE is left replayable as HTTP
says. A drop before the server saw it replays and lands; a drop after it landed
replays to a 404 that reads as done; neither costs a probe. What is given up,
deliberately, is the "resource never existed" signal for a consumer deleting by
a stale id; a consumer that needs it reads first. The copy-then-delete leg of
the MOVE fallback inherits the same rule, so a replayed delete of a source the
first attempt already removed is the move's end state rather than a failed leg.
Pinned by `a_dropped_delete_replays_and_a_gone_target_is_a_success` in both
crates and `a_delete_replays_and_reads_a_gone_target_as_success` in dav-core.

The declaration does two things, and the second is the one that is easy to miss.
On the wire it stops `bifrost-net` replaying the request. At CLASSIFICATION it
also overrides the `AccountOperation` idempotency table, which is the default
source of `derive`'s idempotency and is keyed on the LOGICAL operation rather
than on the request: the copy leg and the MOVE both run under
`EventUpdate` / `ContactUpdate`, which that table calls idempotent, so without
the override a drop on either derives `Retry(SameRequest)`. `dispatch_once`
therefore stamps `idempotency_override(false)` on every failure it returns FROM
THE WIRE, not only the transport one - the oversized-response arm mints
`Protocol(PartialResponse)`, which `derive_protocol` routes by the same flag.
(The two invalid-header returns precede the stamp and do not carry it; harmless,
since a malformed header derives `ClientBug` either way.)
`MOVE` needs the declaration even though it is an extension method net never
replays, precisely because the classification half is separate.

**And the declaration must survive a REBUILD.** That is the third place, and the
easiest to miss. `partial_sequence_error` (caldav) and `partial_move_error`
(both) reclassify a later leg's failure by minting a FRESH
`AccountErrorBuilder::new(Protocol(PartialResponse), ..)` and copying only the
cause chain across - an override is not a cause, so it does not ride along.
Without re-stating it there, `derive_protocol` fell back to the operation table,
called `EventUpdate` / `ContactUpdate` idempotent, and answered
`Retry(SameRequest)` for a half-applied sequence: on a MOVE-less server the
engine re-issues the update, finds no MOVE, create-PUTs the destination the first
attempt already occupied, takes a 412 -> `Retry(AfterStateRefresh)`, and loops
forever - or, before DELETE absorbed a gone target, GETs a 404 and reports a
move that SUCCEEDED as `ProviderRefused`. Both wrappers therefore set
`.idempotency_override(false)` unconditionally: a sequence whose earlier leg
landed cannot be replayed from the top whatever the table says about the logical
operation. RSVP was right only by accident, because `EventRsvp` is
non-idempotent in the table. Pinned by
`a_move_without_server_move_support_copies_then_deletes` and its CardDAV twin,
which assert the `recovery()` and not only the kind.

One failure is deliberately NOT treated as reaching the server: a token source
that cannot mint a credential. `transport_error` stamps `Attempt(Unsent)`, since
no request left the process, so it derives `Retry(SameRequest)` rather than
sending the consumer probing a target nothing was ever written to. That is right
for a transient read failure and wrong for a dead credential, so `auth_headers`
does not fold every `TokenSource::current()` failure into it: a
`bifrost_net::Error::AuthLost` or `RefreshFailed` is handed to
`bifrost_net::into_account_error` - the same funnel the wire path uses - and
comes out `Authentication(ReauthorizationRequired)` or
`Authentication(RefreshTransient)`. Only the remainder keeps the `Unsent`
transport classification. Without the split a permanently revoked bearer token
became an endless retry with no reauthorization signal.

**A `Reconcile(CheckTarget)` must name the target.** `event_create` /
`contact_create` mint the resource URL and the UID locally, so a dropped
create-PUT carries nothing that identifies what to probe - and a consumer told to
check an unnamed target can only re-create, which is the duplicate the whole
declaration exists to prevent. Both create paths therefore stamp the minted URL
as an `ErrorScope` on the failure. Pinned by
`a_dropped_create_names_the_target_to_reconcile_against` in both crates.

Finally the XML decoding primitives, in `bifrost_dav_core::xml`: `local_name`,
`normalize_etag`, `resolve_href`, `push_text`, `trimmed`, plus `escape_xml` and
`append_path`. All were byte-identical.

**The 207 parser, collapsed.** The propstat state machine used to be the one
piece deliberately left behind, on the argument that unifying it was a redesign
rather than a move. The defect ledger overruled that: eight recorded drift
defects lived in those hand-mirrored lines. It is now
`bifrost_dav_core::ResponseParts<P: PropSet>`, driven by
`parse_multistatus(xml, &mut sink)` against a `MultiStatusSink`.

`ResponseParts<P>` owns everything that is not a property name: whether a
`<response>` is open, the `<href>`, the response-level `<status>`, which
propstats succeeded and which failed with what code, the staged-versus-committed
split, and the lane decision. It exposes three rules as methods, and those three
rules are where every one of the drift defects lived:

- `staged_mut` / `marker_mut` plus `commit_propstat` - a property is read into
  the STAGED bag and promoted only when its own propstat answered 2xx. An absent
  status is success; a status that is PRESENT and unparseable is a refusal.
  An empty-element `<status/>` inside a propstat counts as PRESENT. It never
  reaches the `End` arm that reads status text, so left to the sink it looked
  ABSENT and committed the propstat as a success - the exact inverse of the
  rule, and enough to publish properties the server had refused.
  `parse_multistatus`, `extract_href_properties` and
  `parse_collection_property` each refuse the propstat on it. An empty
  `<status/>` directly under `<response>` is unchanged: it leaves the
  response-level status unset and the propstat ladder decides the member.
- `entry_href` / `failed_href` - a response whose ONLY propstat failed is a
  failed href, not an entry; a response with NO propstat at all commits as an
  entry, because dropping a resource the server named out of both lanes makes
  the snapshot diff destroy something that exists.
- `fetched` / `failed_resource` / `missing_data_href` - the multiget lanes,
  including the collection exclusion and the worst-failed-status rule.
  `failed_resource` and `member_status_code` share one private
  `worst_failed_status` with exactly two rungs: the first refusal that is not
  404/410, else document order among the missing-property answers. It is
  deliberately NOT a numeric maximum - `404`/`410` are the only codes
  `FailedResource::is_missing_resource` reads as benign, and among real
  refusals a larger number is not a worse one, so ranking `507` over `401`
  would bury a reauthorization signal under a storage complaint. Inside the
  upper rung the server's own order stands. The rationale in full is at the
  function.
- `failed_member` - the listing lanes' failure entry, an href paired with
  `member_status_code`. Both lanes therefore produce the shared
  `FailedResource`, and both feed the one `classify_207` ladder
  (`MultiStatusOutcome::Usable` / `CompleteFailure { status }`) rather than the
  two hand-mirrored copies each crate carried.

Each crate supplies only a `PropSet` (which properties it stages and how a
successful propstat merges them) and its entry constructors. CalDAV has two:
`CalendarCollectionProps` (`<calendar/>`, the privilege markers, `displayname`,
`calendar-color`, `sync-token`) and `EventProps` (`<collection/>`, `getetag`,
`getcontenttype`, `calendar-data`). Every listing lane goes through the shared
machine: the depth-1 PROPFIND, the `calendar-multiget` / `calendar-query`
REPORT, the `sync-collection` REPORT, and calendar discovery. The depth-0 token
reads (`sync-token` here, `getctag` in CardDAV) share
`parse_collection_property`, and both crates' `current-user-principal` /
home-set walks share `extract_href_property` / `extract_href_properties`.

`take_collection_response` stays here: it is CalDAV-specific post-processing,
dropping the collection's own response from a sync report and reading its RFC
6578 s3.6 `507` as the truncation marker.

**The polling cursor, collapsed.** The second tier went the same way, into
`bifrost_dav_core::snapshot`: `SnapshotEntry`, `encode_snapshot` /
`decode_snapshot` (the length-bounded byte codec, with the entry-count guard and
the trailing-bytes refusal), `diff_snapshots` (including the transient-empty-207
suppression and the failed-href preservation), `preserve_unobserved_entries`,
`inventory_entry`, `object_change`, and the page-cursor trio
`decode_watermark_cursor` / `encode_watermark_cursor` /
`slice_after_watermark`. `page_after_watermark` - the slicer for a lane whose
REPORT already answered with the object bodies - is GONE: every lane pages
before hydrating and builds its own `Page`, because the count it reports and
the failures it carries come from two different legs. It was never published
(this crate is private), and the two rules its tests pinned - a zero page size
is an exhausted page rather than one pointing at its own watermark, and a
watermark past every key is an empty final page - moved onto
`slice_after_watermark`, which is where they actually live. What stays local is what
genuinely differs: the magic bytes (`CALDAVET1` / `CDAVCTAG1`), the
envelope-version and scope validation, the name of the token, and the crate's
own `cursor_error`.

The filtered-query half lives in `bifrost_dav_core::query`: `FilteredHrefs`
(matched, or the server will not run this filter), `HrefQuery` (candidate
hrefs, the 207's refused hrefs, and the worst degraded leg, with the same
`settle` rule the multiget lanes use) and `sorted_candidate_hrefs`. Both crates
supply only the query body and the parser that turns their 207 into hrefs.

So the drift rule from the next section still applies, but to a much smaller
remainder: the query bodies, the property constants, the iCalendar projection,
the scheduling lane, and the handful of error mappings listed below.

Also NOT moved, and for the same reason as `not_found_error`:
`bifrost-carddav` has its own `send_status_request` that returns the response
ETag, where CalDAV's discards it. CardDAV's `put_vcard` and `delete_vcard`
report the new validator to their callers and CalDAV's equivalents do not, so
collapsing the two would have dropped an etag on every CardDAV write.

The two dialects differed in exactly three values - the `ResourceKind` a 404 and
a 403 name, the `Protocol` stamp, and the `field` label on a local argument
error - so those became a `DavProtocol` parameter, and this crate binds it once
as `const DAV: DavProtocol = DavProtocol::CalDav`. That binding is load-bearing
for the crate's entire error surface, so
`every_error_this_crate_mints_is_stamped_caldav` pins it; ablating the constant
before that test existed failed exactly one unrelated assertion.

What deliberately did NOT move: `not_found_error` differs between the crates in
construction, though not in what it stamps. Both carry an `Acknowledged`
attempt, because both are read out of a server answer. CardDAV's local
`not_found_error` decorates the already-classified 404 `status_error` with an
`ErrorScope` for the contact, so the 404's response text survives; CalDAV's
`missing_event_error` is a thin wrapper over
`bifrost_dav_core::not_found_error`, which carries the id in the cause and
mints no body text, and the event scope is attached by the caller
(`event_scope`, on the projection failures as on the GET failure). That is a
real difference in what each error is built from, not drift, and flattening it
would have changed what consumers receive - so each crate keeps its own entry
point.

## This crate and bifrost-carddav are near-duplicates, and drift is the defect

The transport, credential gate, error ladder, 207 parser, polling cursor and
principal discovery walk are all shared through `bifrost-dav-core` now and can
no longer drift - that includes the `ResponseParts` propstat state machine, href
resolution, multiget classification, the cursor codec, the snapshot diff and
the well-known fallback, all of which this section used to list as
hand-mirrored. What remains duplicated is a much smaller remainder: the query
bodies and property constants, the per-domain projections (`ical.rs` /
`vcard.rs`), the discovery step after the principal (which genuinely differs:
a multi-property principal PROPFIND here, an addressbook-home lookup there), the
account-level orchestration around the shared pieces, and the `Unsupported`
stubs each crate carries for the other's domain.

Nothing compares the two copies, so divergence is silent. Five separate defects
in one hardening arc were exactly that: `escape_xml` quoting, the immediate
collection-marker promotion, the eleven hand-maintained CalDAV `propstat_*`
twins, the phantom-collection asymmetry, and `as_fetched_vcard` missing the
`is_collection` guard its CalDAV twin already had (which surfaced an echoed
collection as a phantom card).

**Any fix to shared-shape code in one crate must be checked against the other.**
Where the asymmetry is real - CardDAV has no `sync-collection` path and
discovers addressbook-home-set plus optional directory-gateway properties - it is design, not oversight. Where the fix is to
something both crates route through, it belongs in `bifrost-dav-core` rather
than in one `account.rs`.

Collapsing the two published crates outright into a single `bifrost-dav`
parameterized over the collection kind was considered and NOT taken. It would
remove the remaining duplication, but it reshapes two published pre-1.0
surfaces whose consumers are outside this workspace by definition, and the
duplication count is not an argument that can settle a published-surface
question. The private shared crate was taken instead, on the `bifrost-sasl`
precedent: it kills the drift without either crate's contract moving. Reopening
that choice is a repository-owner decision, not an engineering conclusion.
