# bifrost-carddav reference

Request replay safety is not inferred from the enclosing operation.
`ContactUpdate` includes relocation, whose MOVE and conditional create-PUT legs
are explicitly unreplayable. The error overrides on those legs and on a
partially completed sequence are required even if net is given an operation
label. Create-PUT already explicitly disables net replay; the HTTP PUT default
alone would be too permissive.

Current Stage 3 W2 standalone CardDAV account implementation.

## Public surface

- `CardDavCredentials` - Basic or bearer credentials. `bearer(token)`
  wraps a raw string; `bearer_source(Arc<dyn TokenSource>)` takes a
  shared rotation source. The bearer token is read via `current().await`
  per DAV request (inside the timed dispatcher), so a token rotated mid-sync is
  honored on the next request without reopen. A target 401 forces one
  `refresh_if_unchanged` with the exact rejected token and replays the owned
  request body using the returned token; a second 401 is `AuthLost`. Basic
  credentials are not refreshed. `Clone` only, hand-written
  `Debug` redacting the source; no `PartialEq`/`Eq`.
- `CardDavConfig` - base URL plus credentials (`Debug`/`Clone`, no
  `PartialEq`/`Eq`).
- `CardDavAccountFactory` - implements `AccountFactory`.

`CardDavAccountFactory::open(account_id)` discovers the CardDAV
addressbook home and directory gateways, caches the default personal address book URL, and returns an
`Arc<dyn Account>` inside an `OpenedAccount` whose skip lane is always
empty (single-principal surface). The raw DAV client and parser modules stay
crate-private; consumers use only the factory and the shared `Account`
contact primitives. Discovery tries `/.well-known/carddav` first - built from the ORIGIN of the
configured base URL via `bifrost_net::url::well_known_url`, never by appending
the suffix to a configured path - and falls
back to the configured base URL both when that probe answers that it is not a
discovery endpoint and when its successful body does not identify a current-user
principal. The probe-only fallback triggers are 404, 405, a locally-refused
cross-origin redirect, a redirect the walk will not follow at all
(`Protocol(ContractViolation)`: a chain past the hop cap, or a `Location` that
will not resolve or will not decode), and a body that will not parse as DAV XML
(`Protocol(ParseFailed)`, motivated by the front end answering the origin-root
PROPFIND with `200 text/html` and its index page); 401 and 403 still fail the
open, and `ParseFailed` is deliberately NOT a trigger on the configured-base
leg, where a parse failure is a real contract violation.

The `ParseFailed` arm is deliberately not narrowed: ANY `Protocol(ParseFailed)`
from the well-known principal lookup triggers the fallback, INCLUDING malformed
or truncated DAV XML from a genuine discovery endpoint, because the predicate
cannot tell that apart from a non-DAV body - both arrive as an XML parse failure
with no headers in hand, so the distinction is unobtainable here rather than
merely unimplemented. The fallback cannot accept bad data: it performs a FRESH
principal lookup against the configured base URL, requires its result, and runs
the rest of discovery normally, reusing nothing partially parsed from the probe
and admitting no origin from it, while base-leg and post-principal failures
still propagate. The accepted cost is a lost DIAGNOSTIC - an operator is not
told that the well-known endpoint is serving truncated XML. A document that
parses but names nothing is a different failure: a configured base naming no
current-user-principal, or a principal naming no `addressbook-home-set`, is
`Protocol(MissingField)` through dav-core's `missing_field_error`, not
`ParseFailed`, and is deliberately not an arm of the fallback predicate.
`reference/caldav.md`
carries the same reasoning at length, and the twin must not drift from it.

The walk is no longer a twin at all: it is
`DavDispatch::discover_current_user_principal` in `bifrost-dav-core`
(`discovery.rs`), shared with `bifrost-caldav`, and this crate's
`discover_addressbook_home` calls it and keeps the addressbook-home and directory-gateway lookup
after the principal. `DavProtocol::well_known_service` names the well-known
suffix. Inside the walk one private `discover_principal(root)` runs the PROPFIND
and the `current-user-principal` decode together, returning
`Result<Option<String>, _>`, and both the well-known probe and the
configured-base leg call it. Keeping the decode at the call site is what let
this crate's former copy diverge - it parsed the probe body inside the
`Ok(response)` arm and lifted the failure with `?`, so a probe answer that
would not parse never reached the fallback predicate at all and failed the
open, while the same deployment answering an empty 207 fell back and worked.
One walk removes the second copy that could drift. It is pinned once, for both
dialects, by the dav-core tests `reference/caldav.md` names.

The predicate, `should_fallback_discovery` in `bifrost-dav-core` (`error.rs`),
is parameterized by `DavProtocol` and has one caller, that shared walk. It
falls back on the dialect's own `NotFound` resource kind, a malformed request,
a parse failure, a contract violation and a 405. The contract-violation arm
covers the redirect failures `send_raw_request` now classifies through
`bifrost-net`; they were `Request(Malformed)` and fell back through the
malformed-request arm, so reclassifying them without it would have silently
narrowed the predicate.

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
- `account.rs` - crate-private contact-only `Account` impl.
- `client.rs` - crate-private CardDAV client: discovery, `PROPFIND`,
  `REPORT`, `PUT`, and `DELETE`. Wire traffic rides `bifrost-net` through
  `bifrost-dav-core`'s `DavDispatch` (see `reference/caldav.md`, "The shared
  layer"), so DAV legs share the retry budget, per-host rate limiting,
  bandwidth metering and observability with every other HTTP protocol crate;
  scripted transcripts exercise DAV flows at the wire, below all of it, without
  a listener. DAV still mints its own credentials and walks its own redirects.
  Every request path classifies a non-2xx status before the body is parsed, so
  an error page can never decode as an authoritative empty report - except the
  `href_query_leg` filtered lane, which reads the raw response through
  `report_raw_response` because it must see a refused filter (400/405/501, or a
  403 naming a filter precondition) as a signal to degrade to a local match
  rather than to fail the call. It classifies every other non-2xx.
  A response body exceeding the buffered ceiling is classified as
  `Protocol(PartialResponse)` with `Attempt(Acknowledged)`, so a completed
  non-idempotent mutation reconciles instead of replaying blindly.
  Credential-bearing requests are limited to the configured base origin plus
  an addressbook-home or directory-gateway origin delegated by an authenticated principal on that
  origin. Resource hrefs, consumer-provided native ids, and a cross-origin
  principal href cannot extend that internal origin set. A delegated origin
  may never weaken the transport guarantee the configured base URL
  established: when the base URL is `https`, a discovered `http` home is
  refused admission, so discovery cannot become a downgrade channel for the
  account credential. A cross-origin `https` home is admitted, because a
  principal and an address book home on different hosts of one service is a
  real deployment shape. Discovery stages the home without changing trust;
  it is admitted only after the complete authenticated discovery succeeds,
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
  bifrost-net's `DEFAULT_MAX_HOPS`. A redirect the walk will not follow (past
  that cap, or a `Location` that will not resolve or is not valid UTF-8) is
  classified through `bifrost_net::into_account_error` as the transport
  classifies its own: `Protocol(ContractViolation)` with
  `Attempt(Acknowledged)`, not the `Request(Malformed)` it used to be. The
  refused hop to an unadmitted origin stays a local `Request(Malformed)`, now
  stamped with an `Acknowledged` attempt because the previous hop was answered;
  `reference/caldav.md` carries the detail, including the walk's first-hop
  origin refusal.
- `parse.rs` - XML response parsers for addressbook discovery,
  contact listing, multiget hydration, depth-0 `getctag`, and nested href
  properties. Addressbook/listing/multiget and href-valued discovery
  properties are staged per `propstat` and committed only for successful 2xx
  statuses or a missing status, which RFC 4918 requires but the parser
  tolerates as success. A status that is present but unparseable is NOT a
  missing one and refuses the propstat - including an empty-element
  `<status/>`, which reaches no text-reading arm and so used to look absent and
  commit properties the server had refused. An empty `<status/>` directly under
  `<response>` is unchanged.
  `parse_propfind_contacts` returns a `CardDavContactListing`: committed
  non-collection `entries` plus `failed` (non-collection resources whose only
  propstat failed within the 207, each carrying its member status), so the
  snapshot diff can preserve a transiently-failed resource instead of destroying
  it and the listing classifies through the same RFC 4918 s13 ladder the
  multiget lanes use. `failed_hrefs()` projects that lane onto the bare ids the
  diff guard and `Page::failed_ids` take. A response with NO
  propstat at all commits as an etag-less entry, matching the CalDAV twin:
  requiring a successful propstat dropped a bare
  `<response><href/></response>` out of BOTH lanes - not an entry, and not a
  failed href either - so the contact vanished from the snapshot and the diff
  emitted a `Destroyed` for a row the server still holds, with the failed-href
  preservation guard unable to reach it. Response parsers
  use element-stack parent checks so nested same-name properties do not
  overwrite response-level hrefs. Every text-bearing parser accepts both
  XML text and CDATA. Response hrefs are rebased against the URI of the request
  that produced the multistatus, yielding absolute native URLs at the decode
  boundary before the account layer can consume success or failure lanes. The
  base is the EFFECTIVE request URI: the DAV transport carries the post-redirect
  URL back on every response, because the redirect policy follows admitted-origin hops
  and RFC 4918 resolves against the URI that actually served the body.
  Resource identification does not depend on a `.vcf` suffix or a returned
  content type. Contact PROPFIND, addressbook-query, and multiget request
  `resourcetype`, and all of their
  success and failure lanes exclude responses known to be collections. The
  `collection` marker obeys the same commit-on-success rule as every other
  property: seen inside a `propstat` it is staged and promoted only if that
  block's status was 2xx, so a server echoing the requested prop skeleton
  (`<collection/>` included) back inside a 404 propstat cannot discard a
  contact whose own properties came back 200.
- `vcard.rs` - small vCard projection between DAV resources and
  `bifrost-types` contact cards. **Parse-in** uses caldata's `LineReader`
  for RFC 6350 line unfolding (deletes exactly one leading WSP, not the
  whole run), then a faithful quoted-parameter splitter (`split_content_line`)
  that handles parameter values containing `:`/`;`/`,` and keeps every TYPE
  value (caldata's public `get_param` exposes only the first). vCard 3.0
  bare-param shorthand (`EMAIL;WORK:`) is read as a bare TYPE value.
  Projection is fallible: a malformed body (unterminated quoted parameter,
  missing value, invalid UTF-8 in a folded run) returns a `VCardParseError`.
  Bulk listing/search route a single bad resource into `Page::failed_ids`;
  single-resource get/update surface it through `project_error` as
  `parse_error`: `Protocol(ParseFailed)` with an acknowledged attempt, deriving
  `ProviderContractViolation`, because the card is the server's answer to a
  GET. It was `local_error` (`Request(Malformed)` -> `ClientBug`), which blamed
  the caller for the server's data. Pinned by
  `an_unparseable_card_from_the_server_is_the_providers_fault`, twin of the
  CalDAV test of the same shape.
  **Serialize/patch** keeps the hand-rolled verbatim-preserving splice:
  preserved (unmodeled) lines are re-emitted byte for byte on their physical
  line groups - no unfold/refold - and only freshly emitted lines are folded
  (the 75-octet budget reserves the continuation space). Version is detected
  from the card's VERSION line so emitted TYPE/PREF and PHOTO forms match
  (4.0 `PREF=1` + `PHOTO:data:image/<t>;base64,...`; 3.0 `TYPE=PREF` +
  `PHOTO;ENCODING=b;TYPE=...`). vCard group prefixes (`item1.EMAIL` /
  `item1.X-ABLabel`) are carried onto rewritten EMAIL/TEL/ADR lines so an
  Apple-Contacts label stays bound. ADR po-box/extended components are
  preserved as leading entries of the ordered `street` vector rather than
  zeroed; A TITLE line with no ORG above it maps to an organization with an
  EMPTY name (the shared model has no title-only slot) and writes back as a
  bare TITLE with no invented `ORG:` line - dropped on read, it was also
  deleted by the next `organizations` patch, which strips every ORG/TITLE line
  and re-emits only what the model holds. ORG `;`-structure is preserved (the shared model lacks dedicated
  po-box/extended and ORG-component slots - a types change, out of scope
  here, would make these fully structural). The create path emits a minimal
  `N` (mandatory in 3.0). A present-but-empty value is still
  indistinguishable from absent (the shared limitation calcard also has).

  **The writers refuse what they cannot write; they never strip.** Every value
  the writers emit goes through one of three treatments. TEXT (FN, N, UID, EMAIL,
  TEL, ORG, TITLE, ADR components, NOTE) is escaped by `escape_text`
  behind `text_value`: CR and LF become the `\n` line break, so nothing injects a
  content line. TYPE parameter values (and the 3.0 PHOTO TYPE hint) are RFC 6868
  caret-encoded by `escape_param` behind `param_value`, which carries CR, LF and
  DQUOTE. URI values are not TEXT and have no escape form: the PHOTO URI goes out
  verbatim through `uri_value` (a comma or semicolon in a URL stays bare, as
  RFC 6350 requires), and the 4.0 inline PHOTO media type sits unescaped inside
  a `data:` URI; both refuse every ASCII control character, line breaks
  included. The reader still unescapes a PHOTO URI, so cards an older writer
  TEXT-escaped read correctly; that is lossless because a valid URI carries a
  backslash only percent-encoded. Tab is legal in TEXT and parameters. Any other ASCII control
  character (NUL included) has no escape form in TEXT or a parameter and is
  refused: `vcard_from_create` and `vcard_from_patch` return a `VCardWriteError`
  naming the field, which `write_error` in `account.rs` maps to a local
  `Request(Malformed)` before any request; the value is not echoed into the
  message. Before this a control character passed straight through, and the
  data URI media type accepted a line break that injected arbitrary content
  lines. Pinned by `writers_refuse_control_characters_and_still_encode_line_breaks`.
  Parameter values use RFC 6868 caret encoding in both directions, so a
  quote or newline in a parameter survives a write/read round trip. For
  vCard 4 preference ordinals, the lowest valid ordinal within each
  EMAIL/TEL/ADR group maps to the shared primary flag.
- `capabilities.rs` - contact-only `AccountCapabilities`.

## Account behavior

Supported contact primitives:

- `address_books_list` - `PROPFIND` depth 1 on the discovered
  addressbook home, filtering `resourcetype` entries that contain
  `addressbook`. The PROPFIND requests `current-user-privilege-set` and
  `can_create/update/delete_contacts` are DERIVED from it, as the CalDAV twin
  derives its event flags: a book whose answered privilege set names no write
  privilege is reported read-only, and one the server does not answer for stays
  unknown and is assumed writable. The three flags were hardcoded `true` for a
  long time, so a read-only shared book advertised writable and a consumer's
  capability gate passed a PUT the server was always going to refuse - the same
  defect class as the phantom book below, and one more measured divergence
  between the twins. A home enumerating zero addressbook collections yields an
  EMPTY list, never a fabricated placeholder. The depth-1 parse returns the
  home's own response too, so a home that is itself an addressbook collection
  is already mapped; an empty result therefore means a genuinely empty
  backend, and reporting it as empty lets a consumer reap stale books rather
  than chase a phantom whose queries a spec-correct server 404s. The phantom
  that used to be synthesised here also advertised
  `can_create_contacts: true`, so a consumer that trusted it and POSTed a
  vCard to the home URL earned a 404 or 405 it had done nothing to deserve.
  `bifrost-caldav::calendars_list` removed the identical shape for the
  identical reasons; the two drifted on this for a long time because neither
  side pinned it, and both are pinned now
  (`an_empty_home_lists_no_address_books_rather_than_a_phantom` and its
  CalDAV twin).
- `contacts_list` - `PROPFIND` depth 1 for vCard resources, local
  WATERMARK-cursor slicing of hrefs - sorted by resolved href first, because
  the cursor is local and every page re-runs the PROPFIND, and DAV guarantees
  no multistatus ordering - then batched `addressbook-multiget`
  `REPORT` hydration for only the requested page.
  **There is no request limit, and that is the trait's shape rather than a
  crate omission**: `Account::contacts_list` takes an address book and a page
  cursor and nothing else, so `CONTACT_PAGE_SIZE` (250) is fixed here. Letting
  a caller choose would be a published-signature change in `bifrost-types`.
  `contact_search`, whose request DOES carry a limit, uses the same constant
  only as its default. `next_cursor` carries the
  last href served, not a count into the collection, so a contact filed
  before it between two pages cannot displace the unserved remainder and one
  filed after it is served on a later page. An empty payload is a corrupt
  cursor, refused rather than read as a silent restart from the first
  contact; a watermark past every href is an empty final page that spends no
  multiget at all. The page is re-sorted by `native_id` after hydration,
  since a multiget answers in whatever order it likes. Multiget REPORTs enumerate
  hrefs in the body and use `Depth: 0`; `addressbook-query` uses `Depth: 1`.
  A hydrated vCard that
  will not parse is recorded (by native uri) in `Page::failed_ids` via
  the pure `partition_hydrated_vcards` helper rather than silently
  dropped, so a consumer can tell a transient per-resource hydration
  failure apart from a real remote deletion and preserve the row. Books
  and cards carry `ContactCorpus::Main`; CardDAV has no auto-collected
  corpus. Multiget returns a `CardDavMultigetReport` with successful cards,
  per-resource failures, and `missing_data` (a 2xx response that omitted
  `address-data` - an absence, not a DAV failure). A wholly failed 207 with
  any non-404/410 failure is routed through normal status classification
  rather than returned as an empty page; partial failures feed
  `Page::failed_ids`.
  The depth-1 listing's own failed hrefs feed that lane too, including the
  empty-query `contact_search` path.

  Multiget is chunked and text search runs one REPORT per property, so each
  REPORT is classified independently. A query leg that reports the filter
  unsupported degrades the WHOLE lane to the listing rather than the one
  property: answering out of the properties a server happened to accept would
  narrow the search with no signal to the consumer. Every leg goes through `accumulate_leg`,
  the single funnel each leg result passes through: all four ways a leg can
  fail - transport, a non-2xx status, a body that will not parse, and a 207
  describing complete failure - are folded into `degraded` there, and the
  function returns nothing, so a leg added later has no unrouted path
  available to it. A malformed body is account-authored data, classified and
  survived rather than asserted on. The CalDAV twin is the same shape and the
  two must not drift apart. A leg that fails wholly after other
  legs returned cards no longer aborts the call: the cards are kept, and the
  worst recovery class encountered rides `MultigetFetch::degraded` into
  `Page::skipped_scopes` as an `ErrorScope::ContactCollection` entry, because
  `failed_ids` carries ids without any classification and would have lost the
  reauthorize/retry signal. A refusal with nothing usable anywhere is still
  an `Err`. Ids appear in exactly one lane: `one_outcome_per_id` drops from
  the failure lane anything that materialized in some leg.
- `contact_get` - a plain `GET` of the resource named by the contact id, with
  the validator read from the `ETag` response header and normalized exactly as
  the multiget `getetag` was, so snapshot comparison still compares like with
  like. A missing resource maps to `NotFound(Contact)` scoped to the id; the scope
  decorates the classified 404, so the server's response text and the
  acknowledged attempt survive.
  `contact_update` reads the current card through the same path. This was an
  `addressbook-multiget` REPORT against the collection DERIVED from the resource
  URL until the dav-bug-hunt round: that shape only works where the derivation
  matches the collection the server believes holds the card (nested collections
  and split principal namespaces break it), a `Depth: 0` multiget against a
  derived parent that is not a collection is a 404/501, and some servers reject
  absolute-URI hrefs in a multiget body. The multiget's only advantage was
  carrying the etag in a prop, which the header supplies. CalDAV's `get_event`
  had the simpler shape all along, so this was another measured divergence
  between the twins; pinned by
  `contact_get_addresses_the_resource_with_a_plain_get`, which asserts the
  method and URL against the request transcript.
- `contact_create` - creates a vCard 4.0 resource with a UUID-backed
  `.vcf` path using `PUT`.
- `contact_update` - fetches the current vCard, applies the shared
  `ContactPatch`, preserves unmodeled vCard lines for unrelated field
  changes, and writes the replacement vCard with `If-Match` when a strong
  etag was present. Weak ETags retain their `W/` marker for snapshot
  comparison but make the PUT unconditional because If-Match requires
  strong comparison (RFC 7232), so a weak validator has no conforming
  conditional form. Against a server that only ever emits weak ETags this
  means `contact_update` has no lost-update protection at all; a consumer
  that needs the guarantee needs an application-level revision check. Inline `ContactPatch.photo` replaces or clears vCard
  PHOTO data.

  **A cross-address-book move is PERFORMED**, by the same machinery and with
  the same guarantees as `bifrost-caldav::event_update`; see
  `reference/caldav.md` for the MOVE-then-fallback sequence, the `Overwrite: F`
  and credential-gated `Destination` rules, the `Protocol(PartialResponse)`
  verdict on a failed cleanup leg, and why a move-only patch issues no content
  write. The contact id is the resource URL and `contact_update` returns `()`,
  so the new id reaches the consumer through sync rather than the call.

  This crate refused the relocation until dav-B11 and, unlike its CalDAV twin,
  never pinned the refusal - so the behaviour change broke no test here. Both
  halves are pinned now
  (`contact_update_moves_across_address_books_and_updates_in_place_otherwise`
  and `a_contact_move_without_server_move_support_copies_then_deletes`). That
  gap is another measured divergence between these two crates.
- `contact_delete` - deletes the DAV resource. A target already gone (404 /
  410) is a success with no validator, and the DELETE replays after a
  mid-flight drop; the ruling is in `reference/caldav.md`, "DELETE is
  replayable".
- `contact_search` / `contact_autocomplete` - non-empty searches issue
  CardDAV `addressbook-query` text-match `REPORT`s over eight common vCard
  fields, including ADR postal addresses, asking for `getetag` ONLY.

  **The filter runs on the server, and the page is sliced before anything is
  hydrated.** Requesting `address-data` there is what made this lane
  O(collection) per page. The union of the eight legs is a PREFILTER over
  candidate hrefs; `contact_matches` over the hydrated page is the authority on
  what the page finally carries. The two are not the same predicate - the local
  match reads PROJECTED fields and folds case with Rust's full Unicode rules,
  where `i;unicode-casemap` sees raw vCard properties - and the decision is to
  keep the server side wide and narrow locally, because the reverse direction
  drops matches silently. A page is therefore allowed to be short, or empty,
  while still carrying a cursor.

  **An all-refused candidate 207 is classified, not served as an empty page.**
  The listing failure lane carries each member's status, so the candidate leg
  runs `bifrost_dav_core::classify_207` exactly as the multiget report does and
  a wholly refused query reaches the consumer with its recovery class. That
  member status is `member_status_code`, so a response whose every propstat
  failed reports the first refusal that is not a 404/410, and document order
  only among the missing-property answers. A member
  refused BESIDE members that answered stays a per-id failure on `failed_ids`
  and the page is served. `reference/caldav.md` carries the reasoning; both
  sides are pinned by
  `an_all_refused_query_207_classifies_rather_than_serving_an_empty_page` and
  `a_partly_refused_query_207_still_serves_the_page`.

  **A server that will not run the filter degrades rather than failing**, by
  the shared `bifrost_dav_core::filter_unsupported` rule
  `reference/caldav.md` documents (400, 501, 405, or a 403 naming a filter
  precondition; never a bare 403, never a 401). The degrade lane is the depth-1
  listing - the same walk `contacts_list` and the empty-query lane take - and
  all three sources produce the same `HrefQuery` and run through the same
  `hydrated_contacts_page`. The key is always the resource href, which is also
  the contact's `native_id`, so a search that degrades mid-walk neither
  re-serves nor skips.

  Candidates are sorted (and deduped, since the eight legs name a resource once
  per property it matched) before slicing, so page order is stable while the
  remote result set is unchanged, and the cursor is the href of the last
  contact served. `estimated_total` is the candidate count. A `limit` of zero
  is honored as an exhausted page - no items, no continuation - rather than
  clamped up to one (which served a contact the caller asked not to receive)
  or emitted as an empty page naming its own watermark again (which loops a
  cursor-following consumer forever). The CalDAV twin pins the same rule.

  **An OMITTED `limit` defaults to `CONTACT_PAGE_SIZE` (250).**
  `contact_search` with `limit: None` serves at most 250 candidates and carries
  a `next_cursor` for the remainder, so a consumer that omits the field still
  walks the collection a page at a time. `bifrost-caldav`'s `events_in_range`
  and `event_search` do the same with their own `EVENT_PAGE_SIZE`; the twins
  handle both an omitted and an explicit limit alike. Every page
  reruns the remote search, so `failed_ids` reports what that page's fetch
  observed - a resource that only starts failing on page three is news on
  page three, and one failing throughout is named on every page. The lane is
  a per-page set, not a running tally. Empty search takes the same walk
  `contacts_list` does - page the depth-1 listing, multiget only that page's
  hrefs - differing only in where the page size comes from, so match-all
  behavior is preserved without re-hydrating the whole address book per page. This is the *personal* corpus only; the separate directory path below
  never enumerates these books or filters matches locally.

## Directory gateways

`CARDDAV:directory-gateway` and the `CARDDAV:directory` resourcetype marker
come from **draft-daboo-carddav-directory-gateway-02**, not RFC 6352.
The principal PROPFIND requests `directory-gateway` alongside
`addressbook-home-set`. The plural href extractor reads all successful
property values, resolving them against the effective response URL. There is
no context-root fallback. Discovery returns home plus gateway URLs without
changing trust; `open_with_client` stages admission after authenticated
discovery, then also recognizes directory-marked collections in the home
listing. A listing marker never delegates an origin: it becomes searchable only
when its origin is already trusted through the configured base or principal
discovery. Only the principal's home-set and directory-gateway properties may
extend the origin set. Principal-advertised directory exclusions are seeded
before the first home listing. An admitted home inside such a directory is not
enumerated: it contributes no personal book or default, while bounded directory
lookup remains available. This does not bypass origin refusal for an unadmitted
or downgraded home. HTTPS accounts refuse HTTP delegation, including gateways. Refused
gateways do not enable `pim_methods.directory_search`. Admitted URLs are sorted
and deduplicated by DAV URL identity, including equivalent encoded spellings.

Gateways are read-only lookup scopes, never personal address books, defaults,
or discovered sync scopes. Advertised gateway URLs and directory-marked
collections are remembered for exclusion even when origin admission refused
them. Origin trust, search gateways, capabilities, defaults and the candidate
sync-scope URLs remain open-time snapshots. Scope discovery omits candidates
that later join the monotone refusal set. The refusal set is MONOTONE: every
advertised or marked directory at open, plus every successful directory marker
observed in a later home listing. Markers can add exclusions but cannot remove
one until reopen, even if a later listing omits the marker or collection.
Tightening personal-read and write eligibility is safe live; widening credential
trust or the searchable surface requires authenticated rediscovery on reopen.
Later observations never admit origins or enable directory search.
Create destinations, update sources, relocation destinations and delete
targets inside a gateway return classified Unsupported before any request,
including the GET preceding an update. Membership compares resolved URL
ancestors by DAV identity (same origin and whole path segments), ignoring
query strings; a similarly prefixed sibling path remains writable.

`DavDispatch` invokes the `DavRequestGuard` hook before every wire hop,
checking both the request target and any Destination header after redirect
rebasing. CardDAV installs this hook when the client is constructed. It shares
the monotone directory set with the Account preflight doors. EVERY method and
operation targeting a known directory is refused by default. There is exactly
one exemption: a `DirectorySearch` REPORT whose target is exactly an admitted
search gateway from open. A GET, a gateway descendant, or a later-observed
directory is not exempt, even under the DirectorySearch operation. New
operations need no entry in a guard list to be protected. CalDAV installs no
directory guard.

This covers update pre-fetch GETs, personal reads, sync, discovery and writes,
as well as redirected targets and rebased destinations. Ordinary refusals are
Unsupported with `ErrorScope::DirectoryGateway { url }` identifying the refused
target. A refused later hop preserves Acknowledged attempt evidence for the
preceding answered redirect; the refused hop is never sent.

At the establishment, inventory and changes boundaries, a directory refusal is
translated to `SyncState(ScopeRevoked)` with the ORIGINAL cursor scope and
operation, deriving `Engine(DisableScope(scope))`. The collection is no longer
eligible for personal contact sync: retrying or downgrading a strategy cannot
make directory enumeration appropriate. Quarantining that scope stops the
walk without restarting the account or retrying forever. Inventory emits only
Terminated, with no personal entries, coverage or checkpoint; changes emit no
personal changes or checkpoint. Redirect evidence survives the translation,
and a redirected directory URL never replaces the original scope being
retired. The inventory/establishment snapshot preflights its collection before
requesting the home, and the per-hop guard also catches a directory learned
from that very home response before any member enumeration.

Personal `contacts_list`, `contact_get` and `contact_search` reject known
directory URLs with classified Unsupported before any request, using the same
resolved URL ancestry rule. This includes cached/default ids after a marker.
Directory results are available only through bounded `directory_search`.

`directory_search` is enabled only with an admitted gateway. A false capability
returns Unsupported before cursor validation or I/O. `directory_enumerate`
stays false: empty or whitespace-only queries return Unsupported. A supplied
cursor returns local `Request(Malformed)` because this endpoint has no paging.
An omitted limit defaults to 250; zero returns an empty complete page locally.

A non-empty query issues one depth-1 REPORT per gateway. RFC 6352 governs
`addressbook-query`, `text-match` and truncation: the body requests
`address-data`, has an `anyof` filter over FN, EMAIL, N and NICKNAME with
`i;unicode-casemap` / `contains`, and sets `C:limit/C:nresults` to the caller's
limit. Transport, XML escaping, effective-URL href resolution and vCard
parsing are shared with the contact lanes. Filter refusals (400/405/501, or
403 naming a filter precondition) return Unsupported; other refusals retain
normal status classification. There is no enumerate-and-match fallback and no
`contact_matches` pass.

A response-level 507 for the REPORT request-URI inside a 207 is consumed before
ordinary per-resource classification, with or without
`DAV:number-of-matches-within-limits`: rows survive and `Page::truncated` is
true, never QuotaExhausted for that collection marker. A resource-level 507
still follows ordinary classification. Local overflow beyond the global limit
also sets `truncated`. Every result has `next_cursor: None` and
`estimated_total: None`; terminal does not imply exhaustive. Unparseable cards,
missing address-data and refused resources use `failed_ids`; email-less cards
are skipped.

Directory projection is dedicated to `DirectoryCard` with provider CardDav.
The preferred non-empty email is the key (lowest valid v4 PREF, or v3 preferred
flag, with provider order breaking ties); extra non-empty emails and phones
remain in provider order. Title is the first non-empty parsed TITLE in wire
order; later TITLE lines in the same organization do not replace it. Personal
contacts retain their existing last-TITLE selection per organization: their
single-title field has no first-title contract, so the directory lookup policy
does not change that established projection.
Company is the first structured ORG component;
department joins later non-empty components with `; `. Escaped semicolons stay
inside their original component. The personal `ContactOrganization.name`
projection still joins components into its single string field, and its writer
still cannot distinguish a literal semicolon from a structural separator.
That existing personal-contact representation is unchanged by this directory
path, which retains the original components separately.

Gateway fan-out uses ordered buffering with at most four REPORTs in flight per
call. Merge order is lexical gateway URL, then response order; first wins.
Deduplication uses only the primary email after trimming and Rust Unicode
lowercasing; aliases do not deduplicate distinct rows. The limit is global.
One failed gateway produces a `SkippedScope` naming
`ErrorScope::DirectoryGateway { url }` while other successful gateways remain
usable (even an empty successful gateway counts). If all gateways fail, the
worst recovery error is returned. Skipped scopes and failed ids independently
signal missing observations; `truncated` specifically signals a result limit.


CardDAV maps ADR postal addresses through the shared `ContactAddress`
model and inline PHOTO data through shared `ContactPhoto`, accepting both
the vCard 3.0 `PHOTO;ENCODING=b` form and the vCard 4.0 `PHOTO:data:` URI
form on read and emitting the form matching the card's version on write.
CardDAV preserves inline PHOTO data on unrelated updates.

Cursor support is contact-only. `discover_cursor_scopes` returns one
`CursorScope::Folder(FolderId(collection_href))` per discovered address book,
and NOTHING when the home holds no collections - an empty walk is an empty
backend, and a fabricated home scope would point every cursor and inventory
request at a 404.
`establish_initial_cursor`
builds a hybrid cursor from the address book URL, the collection
`getctag` when present, and a sorted href/etag snapshot. `changes_stream`
first runs a **ctag short-circuit**: a cheap depth-0 `getctag` PROPFIND
(`client.collection_ctag`) resolves the collection's current tag, and when
that matches the prior cursor's it emits an empty batch and carries the
cursor forward, skipping the full depth-1 PROPFIND + diff. Otherwise it
polls the current snapshot and diffs hrefs/etags. Full WebDAV
`sync-collection` parity with CalDAV is a named follow-up.

The poll resolves the ctag **at most once**, and hands the value it already
has to the snapshot as `CtagSource::Known` rather than letting the snapshot
re-derive it. `contact_snapshot` takes a `CtagSource` for exactly this
reason: `Home(home)` picks the tag out of a depth-1 PROPFIND over the
address book home, which is what cursor establishment and inventory want
since they are enumerating anyway, while `Known` spends no request at all.
Re-deriving it from the home made a changed-ctag poll cost three round trips
where two suffice, and that third request grows with the number of address
books rather than staying one collection wide - the CalDAV twin
(`event_snapshot`, taking `home: Option<&str>`) had taken the cheap path
already, so this was drift, not a design difference.
`poll_snapshot_never_relists_the_address_book_home` pins it against the
request transcript.

The depth-0 request is issued whether or not the prior cursor carried a
ctag. A cursor without one previously had to recover it from the home
listing, which is dearer and comes back empty when the collection does not
appear in its own home; asking the collection directly seeds the tag, so a
cursor that starts without one does not stay without one. The PROPFIND-snapshot diff is hardened against
destroy-everything failure modes: an empty multistatus against a populated
prior snapshot suppresses the mass-delete (treated as "no observation"),
and any href in `current.failed_hrefs` is preserved rather than destroyed.
The checkpoint preserves the prior entries and etags for both no-observation
shapes while retaining the prior ctag. The shared snapshot helper returns an
`Observation`; only complete observations advance tokens. Initial snapshots
with refused members carry no ctag, so subsequent polls repeat the listing
rather than short-circuiting. An incomplete poll retains its prior ctag and
retries while the remote ctag differs.
This deliberately means a genuine delete-all is not reported on that poll or a
later empty poll, since the empty observation remains ambiguous.
`inventory_stream` emits contact inventory entries with ETag fingerprints for
the same collection scope and reports full coverage of that address book. A
listing with refused members fails the inventory before emitting entries,
coverage or a checkpoint, using the member status classification.
Legacy `CursorScope::Type(Contact)` calls remain accepted for compatibility,
target the default address book, and report only a `carddav` provider region.
The CardDAV cursor payload is version 2. Version 2 records the request-relative
native-id namespace; version 1 cursors are rejected so a namespace correction
cannot surface as a delete plus create during snapshot diffing. The rejection is
classified `SyncState(SchemaIncompatible)`, deriving to
`Engine(SchemaIncompatible)` rather than a scope restart: only that directive
also deletes the backfill checkpoint, and without the re-walk the objects
already backfilled would keep their pre-correction id spelling.
Folder-scoped cursors must name an equivalent collection URL in their payload;
a mismatch is rejected as schema-incompatible before polling. Legacy type
scopes have no collection URL to compare.
Cursor entry counts are checked against the remaining payload before
allocation. Multiget response hrefs are rebased onto the same resolved
absolute native-id namespace the snapshot, inventory, and changes lanes
use, `failed_ids` included, so a path-only href from the server cannot
give one contact two ids.

All mail, filter, blob, push, calendar, and settings methods return
`AccountErrorKind::Unsupported` stamped with `Protocol::CardDav`.
`push_stream` and `scope_lifecycle_stream` are empty streams.
`set_priority` and `set_bandwidth_cap` forward to `client.net()`, the crate's
`AccountNet` handle, so DAV traffic is scheduled and metered like every other
HTTP protocol crate's. A CardDAV leg composed into an IMAP-shaped account is
handed that account's own `AccountNet` at construction, so it shares the
account's priority, bandwidth meter and cap rather than running unmetered
beside them.

## Sync scope is per address book

All three sync lanes resolve their collection from the folder scope returned by
discovery. An account with three address books therefore has three independent
cursors, inventories, and change streams. The default address book URL remains
the fallback for PIM calls that omit an address book and for legacy type-scoped
cursor calls; it no longer limits discovered sync coverage. Standalone and
IMAP-composed opens consequently have no collection-limit skip entries.

The default is `Option<String>`, and it is `None` when the addressbook home
enumerated no collections. It is deliberately NOT the addressbook home in that
case, for the reasons `reference/caldav.md` gives for its twin: an empty walk
means an empty backend, and addressing the home would send every
collection-less call to a resource a spec-correct server 404s. A call that names
no address book against such an account fails locally with `Request(Malformed)`
-> `ClientBug` before any I/O.

Only the doors taking an `Option<AddressBookId>` reach the default at all -
`contacts_list`, `contact_create` and `contact_search`. `contact_get` and
`contact_update` derive the collection from the resource's own URL via
`bifrost_net::url::parent_collection_url`, which answers for every id that
resolves absolute, so their fallback to the default is unreachable in practice
and kept only as a total match arm.

`open` delegates to `open_with_client` so the whole discovery-to-account path
can be driven against a scripted transport. Both halves are pinned -
`an_empty_home_leaves_no_default_address_book` and
`an_empty_discovery_opens_an_account_with_no_default_address_book`, the latter
being the one that bites if the home fallback is reintroduced at the call site.

Multi-leg addressbook multiget and the eight property-specific text-search
REPORTs are dispatched concurrently, bounded to `MULTIGET_LEG_CONCURRENCY`
in-flight legs and dispatched in order. This is a per-call local policy for
one input-sized fan-out. No account-wide DAV bound exists because no server
advertises one and nothing needs one yet; bifrost-net's `ConcurrencyLimit`
is available if one is ever wanted. Offset continuations still re-run search
so each page reflects a fresh server observation and carries that observation's
failure lanes.

## The shared layer: bifrost-dav-core

The protocol-neutral half of this crate lives in `bifrost-dav-core`, a private
shared crate. Nothing published moved. See `reference/caldav.md` for the full
inventory of what was extracted and why the two dialects reduce to a single
`DavProtocol` parameter; this crate binds it as
`const DAV: DavProtocol = DavProtocol::CardDav`, pinned by
`every_error_this_crate_mints_is_stamped_carddav`.

`CardDavClient` is a newtype over the shared `DavDispatch`, which owns the HTTP
client, credentials, the admitted-origin credential gate, the manual
cross-origin redirect walk and the generic WebDAV verbs. Native-id resolution is
shared too: `DavDispatch::resolve_url` returns an absolute href verbatim, lets a
root-relative href replace the base path per RFC 3986, and resolves a relative
href UNDER the configured base path rather than beside it. `reference/caldav.md`,
"How `resolve_url` turns a native id into a request URL", carries the rule and
the trailing-slash reason it depends on.
`CardDavCredentials` stays published and unchanged; `to_shared` projects it onto
the dispatcher's `DavCredentials`.

Replay policy is shared and lives with the dispatcher: `reference/caldav.md`,
"Which requests are declared unreplayable", is the description. It names the same
call sites on this side - the create-PUT in `put_vcard` (which is also the copy
leg of the MOVE fallback) and `DavDispatch::move_resource` - states that
`If-Match` and unconditional PUTs stay replayable and that DELETE is replayable
with a gone target absorbed as success (`delete_vcard`, dav-D1), and explains that
the declaration overrides the `AccountOperation` idempotency table at
classification as well as suppressing the wire replay, which is what a
`ContactUpdate`-scoped copy leg or MOVE needs. The third leg of that rule lands
in this crate's own `account.rs`: `partial_move_error` rebuilds the error through
a fresh `AccountErrorBuilder`, which carries no override forward, so it re-states
`.idempotency_override(false)` itself - without it a half-applied move derived
`Retry(SameRequest)` and the replay create-PUT the destination it had already
occupied. Pinned here by
`a_create_put_is_not_replayed_after_a_mid_flight_drop`,
`a_dropped_delete_replays_and_a_gone_target_is_a_success`,
`a_dropped_relocate_move_reconciles_rather_than_replaying`,
`an_if_match_update_put_still_replays_after_a_mid_flight_drop` and the
`recovery()` assertion in
`a_contact_move_without_server_move_support_copies_then_deletes`, each the twin
of a CalDAV test of the same name.

The 207 parser and the polling cursor were both collapsed into that shared crate
too; `reference/caldav.md`, "The 207 parser, collapsed" and "The polling cursor,
collapsed", is the description. What this crate supplies is two `PropSet`
implementations - `AddressBookProps` (`<addressbook/>`, `<directory/>`, the privilege markers,
`displayname`, `getctag`) and `ContactProps` (`<collection/>`, `getetag`,
`getcontenttype`, `address-data`) - plus its entry constructors. Every listing
lane runs through `bifrost_dav_core::parse_multistatus`: address-book discovery,
the depth-1 contact PROPFIND, and the `addressbook-multiget` /
`addressbook-query` REPORT. The depth-0 `getctag` read is
`parse_collection_property(xml, "getctag")`, and the snapshot codec, diff,
inventory projection and watermark slicer (`slice_after_watermark`) are the
shared ones, parameterized only by the `CDAVCTAG1` magic and this crate's
`cursor_error`. `CardDavFailedResource` and `MultigetOutcome` are now aliases of
the shared `FailedResource` / `MultiStatusOutcome`, and both member lanes
classify through the one `classify_207`.

The collapse closed one drift on this side: `<addressbook/>` was accepted as a
resourcetype marker no matter where in the prop bag it appeared, where the
CalDAV twin had always required its `<calendar/>` marker to sit inside a
`<resourcetype>`. An `<addressbook/>` named inside `<D:owner>` or a server
extension therefore minted a phantom address book. Both sides now guard, pinned
by `addressbook_element_outside_resourcetype_does_not_mark_a_collection`.

TWO things deliberately stayed local, both behavioural differences rather than
drift, and both of which the extraction would otherwise have silently flattened:

- `not_found_error` decorates the classified 404 with an `ErrorScope` naming the
  contact, keeping the response text, where CalDAV's `missing_event_error`
  wraps the dav-core constructor and puts the id in the cause. Both stamp an
  `Acknowledged` attempt.
- `send_status_request` returns the response ETag. `put_vcard` and
  `delete_vcard` hand that validator back to their callers; the CalDAV
  equivalents return `()`. Using the shared `DavDispatch::send_status_request`
  here would have dropped the etag on every CardDAV write.

## This crate and bifrost-caldav are near-duplicates, and drift is the defect

The two crates used to hand-mirror roughly 1500 lines of DAV machinery, and
nothing compared the copies, so divergence was silent: eight separate defects
were exactly that. The transport, credential gate, error ladder, 207 parser,
polling cursor and principal discovery walk have all been collapsed into
`bifrost-dav-core` and can no longer drift.

Individual divergences are no longer NUMBERED. They used to be ("the eighth
measured divergence", and so on), and the running tally stopped being true: the
ordinals were maintained by hand in two reference documents with no owner, new
divergences landed unnumbered, and the highest ordinal in a durable doc fell
behind the one being quoted elsewhere. A count that is wrong is worse than no
count, because the whole point of counting was to measure how often these two
drift. What matters is the standing conclusion, which does not depend on the
number: this pair drifts repeatedly, it has done so across discovery, privilege
reporting, resource fetch, move fallback and page-size defaults, and every one
was found by comparison rather than by a test. So each divergence is still
recorded where it happened, and still says it is one.

What remains duplicated is smaller but still real: the query bodies and property
constants, the discovery step after the principal (which genuinely differs: an
addressbook-home lookup here, a multi-property principal PROPFIND in CalDAV),
the `Unsupported` stubs each crate
carries for the other's domain, and the account-level orchestration around the
shared pieces. **Any fix to shared-shape code here must still be checked against
`bifrost-caldav`, and vice versa** - and where the fix is to something both
crates route through, it belongs in `bifrost-dav-core` rather than in one
`account.rs`.

## Related providers

The IMAP account composes `bifrost-carddav` when configured with
`ImapAccountConfig::with_carddav(CardDavConfig)`. JMAP, Google
People, and Graph expose their own native contact primitives through
their Account impls. Calendar DAV support is separate in
`bifrost-caldav`; this crate remains contact-only.
