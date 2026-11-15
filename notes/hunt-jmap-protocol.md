# Hunt: jmap-protocol (bifrost-jmap RFC client surface)

Scope read: everything under `crates/jmap/src/` except `sync/`, plus the parts of
`sync/` that consume it (`capabilities.rs`, `error.rs`, `factory.rs`, `push.rs`,
`account.rs`, `pim.rs`, `calendar_ops.rs`) where a question led there. No code
was changed.

Findings are grouped by the three questions. Each one says what the shape is,
where every copy lives, whether the copies already disagree, and where the single
owner should be. Live or latent defects found on the way are collected at the end
under "Lateral findings", separated from the proposals.

The headline, if only three things are done:

1. **Delete the `HttpTransport` generic.** It is a test seam sitting above
   `AccountNet`, exactly the shape `reference/net.md` says each crate grew and
   should not have. ~35 hand-written doubles re-derive the transport contract, one
   classification branch exists only for them, and it is the reason the whole
   `JmapAccount` is declared undrivable. The right seam (`bifrost_net::test_support`
   under a real `ReqwestTransport`) already exists and is already used in
   `transport_reqwest.rs` tests. (S1)
2. **Make per-id answer accounting a core type**, the JMAP analogue of `869ea04f`'s
   `Target` permit. Today seven response types and ~40 sync call sites each decide
   what an id absent from a response means, and they already disagree. (A1)
3. **Read the session once into a validated, typed `SessionLimits` /
   capability table** owned by `core/session.rs`. The meaning of an absent or zero
   core limit is decided in at least eight places, with live disagreements, and the
   session-vs-account placement of capabilities is decided in four (one of them
   wrong against RFC 9404). (D1, D2, S2)

---

## 1. Axes that should be types

### A1. "Was this id answered?" is a per-response, per-caller decision - make it a type

Every JMAP write or lookup response carries one or more id-keyed lanes (success
map, error map, `notFound`, `notParsable`). RFC 8620 promises one answer per
submitted id; servers omit. What an omission means is decided independently by
each response type in `core/` and then decided again by each caller in `sync/`:

| Site | Omitted id becomes |
|---|---|
| `SetResponse::created` / `updated` / `destroyed` | `Error::SetIdUnanswered(id)` |
| `CopyResponse::created` (`*/copy`) | `Error::SetIdUnanswered(id)` |
| `EmailImportResponse::created` | `Error::SetIdUnanswered(id)` |
| `BlobUploadResponse::created` | `SetIdUnanswered`; a `notCreated` entry becomes `Error::NotParsable(format!("Blob {id} not created: {error:?}"))` |
| `CopyBlobResponse::copied` (`Blob/copy`) | `Error::IdNotFound(id)` |
| `ParseResponse::parsed`, `EmailParseResponse::parsed` | `IdNotFound`, or `NotParsable(id)` |
| `GetResponse` | nothing: `notFound` defaults to empty, and the comment tells every caller to reconcile `list + notFound` against what it submitted itself |

Then `sync/error.rs` has to re-decide:

- `into_account_error` maps a bare `SetIdUnanswered` to `set_id_unanswered("*/set",
  id, ctx, guarded = true)` - guessing both the method name and the guard because
  the error does not carry them.
- `set_answer_error(op, method, guarded)` exists so callers can re-supply the method
  and guard the error lost. A caller that forgets it gets the guess.
- `get_id_unanswered`, `get_id_unresolved_after_query`, `require_created_id` /
  `created_id_unusable` and `convert_id_not_found` are the per-lane classifiers;
  `get_id_unanswered` / `set_answer_error` / `not_found()` are referenced at roughly
  forty call sites across `pim.rs`, `contacts.rs`, `calendar_ops.rs`, `filters.rs`,
  `hydrate.rs`, `page_cursor.rs`, `discover.rs`.

They already disagree. The `Error::SetIdUnanswered` doc comment says the create maps
of "`*/copy` and `Blob/upload`" take the partial-response lane. `Blob/copy` does not:
an omitted blob becomes `IdNotFound`, which `convert_id_not_found` turns into a
terminal `Protocol(MissingField)` (no scope on the context) - and that path is live
in production through `copy_attachment_blob` in `sync/pim.rs`. A `Blob/upload`
refusal (`overQuota`, `tooLarge`, `rateLimit`) loses its `SetError` type entirely
and is classified `Protocol(ParseFailed)`, a provider-fault class for a quota
refusal.

Proposed shape: one generic in `core/` that every response's id lanes decode into,
e.g. `IdAnswers<K, T, E>` with `take(&K) -> Answer<T, E>` where
`Answer = Ok(T) | Refused(E) | NotFound | Unanswered`, and the method name and
guard stamped on the request side. Better still, follow `869ea04f`: have the
request builder hand out a non-`Clone` permit per submitted id (`SetRequest::update`
/ `destroy` / `create`, `GetRequest::ids`), and make the response's only accessor
`settle(permit) -> Answer`. Exactly-once accounting of submitted ids then becomes a
compile-time property instead of a convention every caller re-implements, the
"reconcile `list + notFound` yourself" instruction on `GetResponse` disappears, and
`SetIdUnanswered` carries `{ method: &'static str, id, guarded }` so
`set_answer_error` and the `"*/set"` guess both go.

### A2. Omitted / null / value is encoded three different ways, and create shares patch vocabulary

`core/field.rs` defines `Field<T>` (`Omitted | Null | Value`), precisely for this.
It is used by calendar, address book, vacation patch, push-subscription patch and
mailbox `parentId` patch. Elsewhere the same three-state fact is encoded by
sentinels:

- `skip_if_empty_str` / `skip_if_empty_list` / `skip_if_empty_map` /
  `skip_if_zero_date` (`core/set.rs`) and `skip_if_empty_id` (`core/id.rs`): an empty
  string, list, map, epoch timestamp or empty id means "omit". Used by
  `IdentityCreate`/`IdentityPatch`, `PrincipalCreate`, `MailboxCreate`,
  `VacationResponseCreate`, `PushSubscriptionCreate`, `AddressBookCreate`.
  `PrincipalCreate::new` and `MailboxCreate::new` install the sentinels explicitly,
  and `tests.rs::patch_defaults::creates_install_empty_sentinels` pins that they do.
  Consequence: a create cannot send a genuinely empty value (an empty `shareWith`,
  an empty `name`), and `IdentityPatch` cannot clear `replyTo` with `[]` without a
  separate code path (`identity_patch_empty_list_clears_instead_of_omitting`).
- `Option<Role>` with a `Role::None` variant (`mailbox/mod.rs`): `Some(Role::None)`
  means omit (`role_not_set`), `None` serializes as `null`, `Some(role)` is a value.
  So `MailboxPatch::role(Role::None)` clears the role and `MailboxPatch::default()`
  omits it. This is `Field<Role>` spelled inside-out, and `Role::None` is a wire
  vocabulary entry that serializes as `""`.

Separately, the JSON-object macro (`json_object_struct!`) gives Create and Patch
identical setter vocabulary, so `CalendarEventCreate::calendar_id(id, false)` and
`ContactCardCreate::address_book_id(id, false)` put `{"cal-1": null}` into a CREATE.
An `Id[Boolean]` set in a create must contain only `true`; `null` is patch-only
syntax. `tests.rs::patch_object_null_semantics` pins this wire-invalid create.

Owner: finish `Field<T>`, delete the five `skip_if_*` helpers and `Role::None`, and
give membership sets a type of their own (A7) that serializes differently in create
(`IdSet<T>`, values always `true`) and patch (path ops).

### A3. Creation ids and creation references are strings minted in six places

- `SetCreate::create_id` returns `format!("c{id}")` in every object module (eleven
  hand-copied impls plus the macro one).
- `SetRequest::create` / `create_item` use `next_auto_create_id`, whose comment
  records the collision bug length-based assignment caused.
- `EmailImportRequest::email` mints `i{len}` (length-based).
- `BlobUploadRequest::create_from_*` mints `b{len}` (length-based - the scheme
  `next_auto_create_id` was written to replace; safe today only because no
  `create_with_id` exists there).
- References to a creation (`#c0`) are built with `format!("#{}")` in
  `email_submission/set.rs` (two sites) and in `sieve/set.rs`, and in
  `mailbox/set.rs` the reference is minted INTO a server-id type:
  `MailboxId::new(format!("#{parent_id_ref}"))`, `SieveScriptId::new(format!("#..."))`.
  A typed server id that may actually be a creation reference defeats the point of
  the `Id<T>` phantom.
- `email_submission::SetArguments`'s doc comment has the convention backwards
  ("keyed by create-id or by real id with `#` prefix").

Owner: a `CreationId` newtype in `core/`, minted only by the request that owns the
create map, with `.reference() -> CreationRef` (serializes `#cid`), and an
`IdOrRef<T>` for the id-typed fields RFC 8620 lets take a reference (`parentId`,
`emailId`, `onSuccessUpdateEmail` keys, `onSuccessActivateScript`).

### A4. JMAP states are all `String`; the typed `State` alias is dead

`core/id.rs` defines `pub(crate) type State = Id<marker::State>`; nothing uses it.
Object state (`ChangesRequest::since_state`, `SetRequest::if_in_state`,
`GetResponse::state`), query state (`QueryRequest`/`QueryChangesRequest`
`since_query_state`, `QueryResponse::query_state`), session state
(`Response::session_state`), and push state (`PushObject::StateChange::push_state`,
`WebSocketPushEnable::push_state`) are all bare `String`. Passing a `queryState`
as `sinceState`, or an `Email` state as `ifInState` on `Mailbox/set`, compiles. The
sync layer then stores them as `HashMap<String, Option<String>>` (`email_states`,
`mailbox_states`, `lifecycle_mailbox_states` on `JmapAccount`).

Owner: `ObjectState<O>` (phantom on the object, like `Id<T>`), `QueryState<O>`,
`SessionState`, `PushState` in `core/id.rs`. `ChangesMethod::since` and
`define_set_method!`'s `if_in_state` take the object-typed state.

### A5. A capability-scoped account forgets its capability

`Client::primary_account::<C>()` selects the account by capability and then returns
`Account<Tr>`, discarding `C`. `JmapAccount` holds `mail`, `submission`, `vacation`,
`quota`, `sieve`, `contacts`, `calendars` and every foreign account as the same
`MailAccount` type, so handing the mail account to a `CalendarEventGet` compiles
and silently addresses the wrong `accountId` on a server whose capabilities use
different primary accounts. `Request::call` knows `M::Cap` but uses it only to fill
`using`. And `Request::new` defaults to `Session::default_account_id`, the
lowest-URI primary - documented as an open product decision about generic requests.

Owner: `Account<C, Tr>` (or a `CapAccount<C>`), with `call<M: JmapMethod<Cap = C>>`
or a `ServedBy<C>` bound for methods any account serves (`Blob/copy`, core). With
every request built from a capability-scoped account, the generic default-account
fallback has no caller and the open question dissolves rather than needing a ruling.

### A6. Patch paths are strings with an escaping rule held by comment

`escape_json_pointer_token`'s doc says "every `format!`-built dotted path in this
crate must route its interpolated token through here". The rule is enforced by
reading: paths are built in `EmailPatch::mailbox_id`, `EmailPatch::keyword`,
`MailboxPatch::acl`, `MailboxPatch::acl_set`, `CalendarEventPatch::calendar_id`,
`ContactCardPatch::address_book_id`, and `sync/calendar_ops.rs`
(`participants/{}/participationStatus`). The JSON-object patches also expose
`set_property(name: impl Into<String>, ..)`, which accepts any unescaped path (the
tests use it that way). The RFC 8620 s5.3 rule "a patch may not name a property and
a path into it" is implemented separately as `EmailPatch::remove_patch_entries`, as
`.retain(|name, _| !name.starts_with("calendarIds/"))` and the `addressBookIds/`
twin, and by hand in each single-path setter.

Owner: a `PatchPath` (segments, escaped at `Display`) and one `PatchObject` that
owns both rules, used by `EmailPatch`, `MailboxPatch`'s `acl_patch`, and the
JSON-object patches.

### A7. Smaller axes travelling as primitives

- **HTTP method as `&str`.** `ReqwestTransport::send` / `build_request` take
  `method: &str` and return `TransportError::invariant("unsupported HTTP method")`
  for anything but `"GET"`/`"POST"`. A two-variant enum (or `http::Method`) removes
  the runtime invariant.
- **Keywords as `String`, membership as `HashMap<MailboxId, bool>`.** Keywords have a
  grammar (IMAP atom, length bound) and the workspace treats flag identity as
  ASCII-case-insensitive (`FlagOp`); a `Keyword` newtype in `email/` would own both.
  `mailboxIds` / `keywords` / `calendarIds` / `addressBookIds` values are always
  `true`; they are sets.
- **Account ids as `String` past the request boundary.** `Session::accounts`,
  `primary_accounts`, `PushObject::StateChange::changed`, `Changes`,
  `EmailPush::account_id`, `CalendarAlert::account_id`, and on the sync side
  `foreign_mail: HashMap<String, MailAccount>`. `AccountId` exists and is used only
  inside method structs.
- **`Blob/lookup` `typeNames: Vec<String>`** while `DataType` exists; the doc says
  the caller must add the matching capabilities to `using` and points at a
  `lookup_blob_with_capabilities` helper that does not exist. `DataType -> Capability`
  is exactly the mapping a typed field would let the request add itself.
- **Query position** is `i32` on the request and `i32` on the response (RFC:
  response `position` is `UnsignedInt`), with `i32::try_from` at the sync call
  sites. A `QueryPosition` owning the conversion would end the ceremony `cdfcaa4f`
  partly folded.
- **JSCalendar values as `String`.** `CalendarEventCreate::start`, `duration`,
  `GetArguments::recurrence_overrides_before`, filter bounds are all strings; the
  LocalDateTime / UTCDateTime / Duration grammar lives as `strftime`/`strptime`
  format strings and a hand parser in `sync/calendar_ops.rs`. These belong as types
  in `calendar_event/` (see L5 for the defect this has already produced).
- **`from_timestamp(i64) -> Timestamp`** (`core/set.rs`) returns `UNIX_EPOCH` for an
  out-of-range input; `EmailCreate::sent_at` / `received_at` and
  `EmailImport::received_at` take `i64` and therefore can fabricate a 1970 date.
  Take `Timestamp`.

### A8. Failures that reach the caller only as prose, or with the wrong provenance

- `Error::NotParsable(String)` is overloaded: "this blob id is not parsable"
  (`ParseResponse`), "Blob {id} not created: {debug}" (`BlobUploadResponse`), and
  "Blob/copy response named accounts other than the requested source and
  destination" (`sync/pim.rs::copy_attachment_blob`). The last is a provider
  contract violation; all three classify as `Protocol(ParseFailed)`.
- `From<serde_json::Error> for Error` maps every stray `?` to `ResponseDecode`. To
  compensate, `JmapErrorContext` carries a `send_boundary: bool` and
  `into_account_error` adds `Attempt(Acknowledged)` only when the caller set it. The
  provenance of the decode (did it come from the received envelope?) is a fact at the
  source and should be a variant: `EnvelopeDecode` (from `Client::send_request` / the
  WS frame), `ResultDecode { call_id }` (from `Response::get`), and no blanket `From`.
- `SetResponse::unwrap_update_errors` / `unwrap_create_errors` return the first
  error from a `HashMap` iteration, dropping which id failed and every other error.
- `TransportError { message: String, .. }` with `TransportError::new(message)` and no
  `net` evidence classifies as a generic network fault (see S1); the message is the
  only carrier.

---

## 2. Decisions made in more than one place

### D1. What an absent or zero core limit means

RFC 8620 s2 core limits are read raw (`CoreCapabilities` fields are
`Option<usize>`) and interpreted by each reader:

| Reader | `maxCallsInRequest` / `maxSizeRequest` etc. absent | zero |
|---|---|---|
| `core/request.rs::CallLimit` | `Unadvertised`: no limit enforced | `Invalid`: not enforced |
| `sync/factory.rs::batched_open_probes` | "not a licence to batch": one call per request | (`max_calls < 2`): one call per request |
| `client_ws.rs::encode_ws_request` (`maxSizeRequest`) | not enforced | not enforced (`max > 0`) |
| `blob/upload.rs::upload_to` (`maxSizeUpload`) | not enforced | enforced (every non-empty upload refused) |
| `Session::api_request_concurrency` / `upload_concurrency` | 1 | 1 |
| `sync/capabilities.rs::validate_session` | refuses open (calls, get, set, size, concurrency, collations) | refuses calls/get/set/size; accepts upload and concurrency zero as valid `UnsignedInt` |
| `validate_session` again, after refusing | `.unwrap_or(1).max(1)` on get/set (dead defaults restating a third reading) | |
| `sync/capabilities.rs::upload_size_limit` | refuses | accepts |

They disagree on the same session today: with `maxCallsInRequest` omitted,
`CallLimit` lets a request grow without bound while `batched_open_probes` refuses to
put two calls in one; `validate_session` then refuses the account, so the first two
answers are only reachable before validation runs (the `CallLimit` doc says this is
deliberate, because probes run before validation). The fact that open-probes run
against an unvalidated session is itself the reason three of these readers exist.

Owner: validate once, at `SessionState::derive` (which already runs on every
installed session), producing a typed `CoreLimits` with `NonZeroUsize` where the
RFC or this crate requires positive, and an explicit `Unbounded`/`Serial` reading
where it does not. Every reader takes that struct; `CallLimit`, the WS `max > 0`
check, the concurrency `max(1)`s and `validate_session`'s core half fold into it.
`sync::capabilities` keeps only the capability-to-`AccountCapabilities` mapping.

### D2. Session-level vs account-level capability placement

Which capability URIs are read from `session.capabilities` and which from the
primary account's `accountCapabilities` is decided in four places:

- `deserialize_session_capabilities_map`: decodes core, websocket, quota, blob at
  session level; treats mail, submission, sieve, contacts, calendars, principals,
  principals:owner as account-level markers.
- `Session::capability_config`: its own copy of the same four-URI list.
- The `session_cap_accessor!` invocations: `blob_capabilities` / `quota_capabilities`
  / `websocket_capabilities` / `core_capabilities` read the session map; the six
  hand-written `*_capability_state` methods on `Session` read the primary account
  (each repeating the same "session-level Malformed check, then primary lookup"
  body).
- `core/capability.rs`'s trait doc: "`Config` describes the session object for Core
  and WebSocket and the account object for account-scoped extensions" - which
  disagrees with the code for quota and blob.

The code is wrong for blob: RFC 9404 s3 makes the session-level
`urn:ietf:params:jmap:blob` value an empty object and puts `maxSizeBlobSet`,
`maxDataSources`, `supportedTypeNames`, `supportedDigestAlgorithms` in
`accountCapabilities`. `Session::blob_capabilities` therefore always returns
all-defaults (see L2). The comment in `deserialize_session_capabilities_map` even
states the rule the blob arm breaks ("their account-specific fields must never be
decoded as limits ... for an arbitrary account").

Owner: put the placement on the `Capability` trait (`const LEVEL:
CapabilityLevel`) along with the `Capabilities` variant it decodes into, and drive
the deserializer, `capability_config` and the accessors from it. One macro over
the `Capability` markers would replace the six hand-written account accessors.
`capability_config`'s serialize-then-deserialize round trip of an already-typed
variant goes away with it.

### D3. Does this request fit `maxSizeRequest`?

The WebSocket door always checks the encoded frame (`encode_ws_request`). The HTTP
door checks only when a caller opts into `Request::send_methods_within(max_size)`,
which one call site in the workspace does (`batched_open_probes`). Every other HTTP
batch is sized by `maxObjectsInSet` / `maxObjectsInGet` chunking and otherwise relies
on the server's request-level `limit` rejection. Owner: `Client::send_request`
checks against the request's own session snapshot, as the WS door does;
`send_methods_within`'s `Ok(None)` stays as the "tell me instead of failing"
variant of the same check.

### D4. How a push object decomposes into state changes and token-free notifications

Two traversals answer it with different semantics:

- `event_source/stream.rs::flatten_push_object`: worklist over `Group`, MERGES every
  `StateChange` into one notification carrying the block's resume token, emits
  `CalendarAlert` and `EmailPush` as their own notifications first, and documents
  why that ordering is load-bearing.
- `sync/push.rs::collect_push` + `last_push_state`: recursion over `Group`, routes
  each state change per account, and sends `CalendarAlert` and `EmailPush` to the
  `_ => HintPayload::Unknown` arm (a broad reconcile); `last_push_state` takes the
  last `pushState` in depth-first order.

Its doc says the SSE traversal is "the same traversal" as the WS one; it is not.
On top of that the `StateChange` shape is declared three times:
`PushObject::StateChange`, `client_ws.rs::WebSocketStateChange` (needed because of
the `@type` framing), and `event_source::Changes`.

Owner: one `PushObject::decompose(self) -> PushBlock { notifications, state:
Option<MergedStateChange>, push_state }` in the crate root (or a `core/push.rs`),
consumed by both lanes; `WebSocketStateChange` decodes straight into the shared
struct.

### D5. URI template expansion and value encoding

The session's templates are expanded by three hand loops - `Client::blob_download_url`,
`Client::upload_to`, `Client::event_source` - each matching `URLPart` and choosing
an encoder. Two encoding sets answer "what to percent-encode in a template value":
`core/session.rs::TEMPLATE_VALUE` (`NON_ALPHANUMERIC` minus `-._~`, whose doc
explicitly calls a deny list "the wrong shape") and `bifrost_net::url::COMPONENT`
(a deny list) reached through `encode_template_value_at`. They agree on ASCII today
by enumeration, not by construction. `event_source`'s `Types` arm also re-tests
`contains('?') || contains('#')` on the prefix before calling
`encode_template_value_at`, which tests the same condition internally - and so in
query position uses `TEMPLATE_VALUE` where blob URLs use `COMPONENT`.

Owner: one `expand<P>(parts: &[URLPart<P>], value: impl Fn(&P) -> Cow<str>) -> String`
next to `URLPart::parse`, with one encoder (bifrost-net's, since net owns URL
components workspace-wide, per the `77df77df` precedent). `TEMPLATE_VALUE` and
`encode_template_value` go.

### D6. What credential a request presents

- HTTP bearer: injected by `AccountNet` from the token source.
- HTTP Basic: `ReqwestTransport::build_request` formats the header.
- SSE: `ReqwestTransport::open_sse` repeats the default-header loop and the Basic
  branch of `build_request` line for line.
- WebSocket: `Client::connect_ws` reads the bearer source itself, formats the
  header, and runs its own one-shot 401 `refresh_if_unchanged` retry; Basic goes
  through `Authorization::header_value`.

The `Authorization` value is also held twice: once inside `ReqwestTransport` (HTTP)
and once on `ClientInner` (WS). `ClientBuilder::connect` clones one into both;
`Client::with_transport` fabricates `Authorization::Basic(String::new())` for the WS
copy, so a client built that way presents `Basic ` on the WebSocket whatever the
transport uses for HTTP. Owner: the transport owns the credential and exposes one
`authorize(builder)` used by all three HTTP doors and a `ws_authorization()` the
handshake calls; `ClientInner::authorization` is deleted.

### D7. Setting one member of an id-set without clobbering the rest

`EmailPatch::mailbox_id` / `mailbox_ids`, `CalendarEventPatch::calendar_id` /
`calendar_ids`, `ContactCardPatch::address_book_id` / `address_book_ids`, and the
Create-side twins each implement "path op vs wholesale replace, and never both"
independently; the create twins diverge (A2). One `IdSetPatch<T>` / `IdSet<T>` pair
(with A6's `PatchPath`) owns it.

### D8. Data type, capability and method name are restated at every invocation

`DataType` (crate root) is a string table parsed by hand; `Object` types do not know
their `DataType`; each `define_*_method!(EmailGet, Email, "Email/get",
capability::Mail)` restates the type name and the capability, five or six times per
object; `sync/push.rs::scope_for_data_type` maps `DataType` back to cursor scopes.
`tests.rs::method_name_and_capability_table` is the pairwise-agreement test holding
the copies in step. Owner: `Object` gains `const DATA_TYPE: DataType` (or a
`TYPE_NAME`) and `type Cap`; the macros derive `NAME` (`concat!(stringify!..)`) and
`Cap` from the object, and the table test becomes unnecessary rather than
load-bearing.

### D9. Wire property names, twice per enum

Most object `Property` enums declare each wire name twice: in a
`#[serde(rename = "...")]` and again in a hand-written `Display` match (mailbox,
email_submission, principal, push_subscription, calendar, address_book, sieve,
vacation, identity, ...). `Display` is what `SetError<O::Property>::to_string_error`
and `properties` lists use; serde is what decode uses. `define_open_property_enum!`
already generates both from one table for calendar event and contact card. Using it
(or a closed variant of it) everywhere removes the second answer. `tests.rs` has
round-trip tests per enum that exist only because there are two answers.

---

## 3. Structure

### S1. `HttpTransport` is the wrong seam; delete it and the `T` parameter

`core/transport.rs::HttpTransport` has one production implementor
(`ReqwestTransport`) and about thirty-five test implementors across `client.rs`,
`core/tests.rs`, `blob/`, `event_source/`, and twenty more in `sync/`. Each double
stubs `upload`, `download`, `get_session` and `api_request` and returns
`TransportError::new("...")` with no `net` evidence. Consequences:

- **A classification path only tests reach.** `sync/error.rs::convert_transport`'s
  "custom transport without bifrost-net evidence" branch classifies those errors as
  `Transport(Network)`. Production never produces a `TransportError` without `net`
  or `local`. The doubles exercise a copy of the status contract instead of
  bifrost-net's - the exact situation `reference/net.md` describes as the reason
  `bifrost_net::test_support` was published ("each re-derived this crate's status
  contract, and a mis-derivation is invisible until it hides a live defect").
- **The generic is why `JmapAccount` is "undrivable".** The comment above
  `MailAccount` in `sync/account.rs` says no scripted transport can be installed at
  that level and closing the residual "means threading the transport generic through
  `JmapAccount`". It does not: `ReqwestTransport::scripted_with_retry` (cfg(test), in
  `transport_reqwest.rs`) already builds the production transport over
  `bifrost_net::test_support::ScriptedDispatch`, and `capability_contract_tests.rs`
  already constructs a real `ReqwestTransport` for a whole `JmapAccount`. Installing a
  scripted net there makes every `Account` door drivable hermetically, through the
  real retry budget, replay policy, concurrency gate and bandwidth meter.
- **Three traits for one transport.** `SseTransport` and `blob::download::BlobTransport`
  exist so the generic can be partially implemented; both have only the reqwest
  implementor.
- **Every type carries `T`.** `Client<T>`, `Request<'x, T>`, `Account<Tr>`,
  `MethodTuple::add_to_request<T>`, and every generic helper in `sync/`.

Proposal: make `Client` concrete over `ReqwestTransport` (or over `AccountNet`
directly), delete the three traits, `convert_transport`'s no-net branch, and the
doubles; port tests to `scripted_net`. The WebSocket push lane keeps its own seam
(`sync/push.rs::PushTransport`), which is the genuinely non-HTTP part. This is the
largest single simplification available in the crate and it improves test fidelity
at the same time.

### S2. Session interpretation lives in the engine-facing layer

`validate_session`, `upload_size_limit`, the "which capabilities are malformed and
which matter" policy, and the `CoreLimits` type live in `sync/capabilities.rs`,
interleaved with the mapping to `bifrost_types::AccountCapabilities`. They are a
reading of RFC 8620 s2, not of the bifrost account contract, and they run after
`Client` has already acted on the same session unvalidated (D1). Move them into
`core/session.rs` as `SessionState::derive -> ValidatedSession` (typed limits,
typed capability table per D2), returning a protocol error the sync layer
classifies. `sync/capabilities.rs` keeps the `AccountCapabilities` mapping only.

### S3. `Client` does four jobs

`ClientInner` holds the session cell and refresh gate, the transport, the WebSocket
sink plus the RFC 8887 request-id counter plus the pending-response map, a second
copy of the credential (D6), and the timeout; `Client` adds the per-handle
`ByteTally`. The WS fields are `#[cfg(feature = "websockets")]` struct members and
`client_ws.rs` reaches into `Request`'s `pub(crate)` `using` / `method_calls` /
`created_ids` fields to re-encode the envelope with `serde_json::to_value` (a second
encoder of the request body beside `Request`'s `Serialize` impl). A split into
`SessionCell` (state, refresh, divergence watch), the transport, and a
`WsConnection` (sink, ids, pending, generation) owned by the push layer would put
state next to the behaviour acting on it; the request envelope should have one
encoder that both doors call, with the WS frame wrapping it.

### S4. Vendor and RFC shapes are mixed in one type

`principal/mod.rs::Principal` mixes RFC 9670 (`accounts`, `capabilities`) with a
server-admin dialect (`secret`, `dkim`, `quota`, `members`, `acl`, principal types
`domain` and `list`). The RFC-named property `timeZone` is declared as `timezone`
(see L7). If the admin dialect is wanted it belongs in its own type behind its own
capability, so the RFC object decodes strictly.

### S5. Rights vocabularies live in the wrong module

`principal::ACL` is the Mailbox `shareWith` rights vocabulary (RFC 9670 s4 applied
to RFC 8621 mailboxes) but lives in the principal module and is imported by
`mailbox`. `mailbox::MailboxRights` is the same vocabulary again as a struct of
bools and lacks `mayShare`, which RFC 9670 adds to `myRights`, so a sharing-aware
caller cannot read it. Move `ACL` next to `Mailbox` and derive `MailboxRights` from
it.

### S6. Test layout mirrors history, not the code

`src/tests.rs` (about 3,800 lines) is a numbered list of sections, one per past
review pass, each `#[cfg(...)]`-gated on feature combinations and testing modules
it does not sit beside (mailbox wire, email submission wire, session capability
fallbacks, URL template parsing, blob management, calendar, contacts, principal,
quota ...). `core/tests.rs` (about 1,100 lines) is the same for core. Several
sections exist only to hold two copies in step (D8, D9). Wire tests should live in
the module whose wire shape they pin; the agreement tables go when their
duplication goes. At least one section pins a defect (A2,
`patch_object_null_semantics`).

### S7. The crate-wide `dead_code` allowance hides which half of each module is live

`lib.rs` has `#![allow(dead_code)]` and most object modules repeat it with a
"kept for completeness" comment (email, mailbox, identity, vacation, sieve,
principal, push_subscription, email_submission, event_source). The SSE lane is
unwired but its push decomposition disagrees with the live WS one (D4), so "kept
built" has not meant "kept correct". Not a deletion proposal - everything here is
`pub(crate)`, but removal still deserves an owner's ruling - only an observation
that the unwired halves are where duplicated decisions have drifted, so any that
are kept should share the live path's owners (D4, D5, D6) rather than their own.

---

## Lateral findings (defects, separated from the proposals above)

**L1. Probable live: a server sending `"maxMailboxDepth": null` cannot be opened.**
RFC 8621 s1.3.1 types `maxMailboxDepth` as `UnsignedInt|null`. `MailCapabilities`
declares it `usize` under a struct-level `#[serde(default)]`; `default` covers only
an absent member, so `null` fails the typed parse, `try_cap!` stores the primary
account's mail block as `Capabilities::Malformed`, `malformed_capabilities()` yields
the mail URI, and `validate_session` refuses the account with
`Protocol(ContractViolation)`. No fixture in the crate sends `null`. The same struct
defaults the REQUIRED `maxSizeMailboxName` / `maxSizeAttachmentsPerEmail` /
`emailQuerySortOptions` / `mayCreateTopLevelMailbox` to zero/empty/false when absent,
the merge the `CoreCapabilities` doc comment explains was removed for core.

**L2. Latent: blob capabilities are read from the wrong level.** See D2. No
production reader of `blob_capabilities()` exists today, so nothing misbehaves yet;
any future `Blob/upload` size gate would read `None`.

**L3. Latent: `Blob/upload` refusals classify as provider parse failures.** See A1/A8.
`BlobUploadRequest` has no production caller.

**L4. Live: an omitted `Blob/copy` answer is terminal.** `copy_attachment_blob`
(`sync/pim.rs`) maps `CopyBlobResponse::copied`'s `IdNotFound` through
`into_account_error` without a scope, giving `Protocol(MissingField)`; the sibling
write lookups give retryable `Protocol(PartialResponse)`. The same function reports a
response naming the wrong accounts as `NotParsable` prose (`ParseFailed`) rather
than a contract violation.

**L5. Live (sync scope): a JSCalendar duration in weeks reads as zero.**
`sync/calendar_ops.rs::parse_duration_seconds` rejects `W` (RFC 8984 Duration allows
`P1W`), and `end_from_start_duration` does `.unwrap_or(0)`, so a one-week event is
presented as ending at its start. The `start.len() == 10` all-day heuristic beside it
is another string-shape decision a `LocalDateTime` type would own (A7).

**L6. Live wire defect, pinned by a test: `calendar_id(id, false)` on a create emits
`null`.** See A2.

**L7. Principal `timeZone` never decodes.** The field is serialized as `timezone`;
RFC 9670 s3 spells it `timeZone`, so a conforming server's value is silently dropped
into an `Option` that stays `None`.

**L8. `EmailSubmission` `onSuccessUpdateEmail` overwrite.**
`SetArguments::on_success_update_email_` does `insert(id, EmailPatch::default())`, so
a second call for the same submission discards the first patch rather than handing
it back (contrast `SetRequest::create_with_id`'s get-or-insert, whose comment names
this exact hazard).

**L9. Conditional: WebSocket `CalendarAlert` and `EmailPush` frames trigger a broad
reconcile.** `collect_push`'s `_ => HintPayload::Unknown` arm receives both. If a
server delivers alerts on the push channel, every fired alert costs a full repoll.
See D4.

**L10. `Client::with_transport` fabricates a `Basic ` WebSocket credential.** See D6.
Only reachable by a caller combining `with_transport` with `connect_ws`.

**L11. `from_timestamp` fabricates `UNIX_EPOCH`.** See A7.

**L12. Doc drift worth noting while in the area:** the `Capability` trait doc
disagrees with the code about which capabilities are session-level (D2); the
`BlobLookupRequest` doc names a helper that does not exist (A7); the
`email_submission::SetArguments` doc inverts the `#` convention (A3); the
`event_source/stream.rs` doc calls its traversal the same as the WS lane's (D4).
